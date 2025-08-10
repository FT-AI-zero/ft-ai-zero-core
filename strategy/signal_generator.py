import os
import time
import sqlite3
import numpy as np
from utils.config import DB_DIR, SIGNAL_POOL_DB

PERIODS = ["1m", "3m", "5m", "15m", "1H", "4H", "1D"]
TRADE_SIGNAL_WINDOW_SEC = 30
ORDERBOOK_SIGNAL_WINDOW_SEC = 30
KLINE_PERIOD_WINDOW = {"1m": 90, "3m": 200, "5m": 300, "15m": 900, "1H": 2000, "4H": 9000, "1D": 90000}

def ensure_signal_pool_table():
    conn = sqlite3.connect(SIGNAL_POOL_DB)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS signals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        instId TEXT, period TEXT, ts INTEGER, close REAL, vol REAL,
        signal_type TEXT, status TEXT DEFAULT 'WAIT_SIMU', score REAL, params TEXT,
        created_at INTEGER, source TEXT, priority INTEGER DEFAULT 3,
        promotion_level INTEGER DEFAULT 0, expire_ts INTEGER, trace_log TEXT,
        source_tag TEXT DEFAULT 'signal_engine'
    )""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS signal_cursor (
        instId TEXT, period TEXT, signal_type TEXT, last_ts INTEGER,
        PRIMARY KEY (instId, period, signal_type)
    )""")
    conn.commit()
    conn.close()

def get_signal_cursor(instId, period, signal_type):
    conn = sqlite3.connect(SIGNAL_POOL_DB)
    cur = conn.cursor()
    cur.execute("SELECT last_ts FROM signal_cursor WHERE instId=? AND period=? AND signal_type=?",
                (instId, period, signal_type))
    row = cur.fetchone()
    conn.close()
    return row[0] if row else 0

def update_signal_cursor(instId, period, signal_type, last_ts):
    conn = sqlite3.connect(SIGNAL_POOL_DB)
    cur = conn.cursor()
    cur.execute("""
        INSERT OR REPLACE INTO signal_cursor (instId, period, signal_type, last_ts)
        VALUES (?, ?, ?, ?)""", (instId, period, signal_type, last_ts))
    conn.commit()
    conn.close()

def is_real_symbol(symbol):
    if not symbol or not isinstance(symbol, str): return False
    s = symbol.upper()
    if "-" not in s: return False
    if any(kw in s for kw in ['EXAMPLE', 'DEFAULT', 'RANDOM', 'TEST']): return False
    allowed_ends = ["-USDT", "-USDT-SWAP", "-USD", "-USD-SWAP"]
    return any(s.endswith(e) for e in allowed_ends)

def get_strategy_symbols(period):
    dbfile = f'kline_{period}.db'
    tablename = f'kline_{period}'
    path = os.path.join(DB_DIR, dbfile)
    if not os.path.exists(path): return []
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    cur.execute(f"SELECT DISTINCT instId FROM {tablename}")
    symbols = [row[0] for row in cur.fetchall() if is_real_symbol(row[0])]
    conn.close()
    return symbols

def get_latest_kline(instId, period, window=20):
    dbfile = f'kline_{period}.db'
    tablename = f'kline_{period}'
    path = os.path.join(DB_DIR, dbfile)
    if not os.path.exists(path): return []
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    cur.execute(f"SELECT ts, close FROM {tablename} WHERE instId=? ORDER BY ts ASC LIMIT ?", (instId, window))
    rows = cur.fetchall()
    conn.close()
    return rows

def simple_ma(series, window=5):
    if len(series) < window: return None
    return np.mean(series[-window:])

def simple_rsi(series, window=14):
    if len(series) < window+1: return None
    diff = np.diff(series)
    gain_vals = [x for x in diff[-window:] if x > 0]
    loss_vals = [x for x in diff[-window:] if x < 0]
    gain = np.mean(gain_vals) if gain_vals else 1e-6
    loss = abs(np.mean(loss_vals)) if loss_vals else 1e-6
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

def get_signal_priority_and_promotion(signal_type):
    if signal_type in ["STOP_LOSS", "FORCE_CLOSE", "LIQUIDATION_LONG", "LIQUIDATION_SHORT"]:
        return 1, 1
    if signal_type in ["BREAKOUT_UP", "BREAKOUT_DOWN", "ORDERBOOK_BUY_PRESSURE", "ORDERBOOK_SELL_PRESSURE"]:
        return 2, 1
    if signal_type in ["FUNDING_SPIKE", "WHALE_BUY", "WHALE_SELL"]:
        return 3, 0
    return 4, 0

def insert_signal(instId, period, ts, close, vol, signal_type, score=7.0, params=None, source="signal_engine_v2"):
    now_ts = int(time.time())
    expire_ts = now_ts + 10
    priority, promotion_level = get_signal_priority_and_promotion(signal_type)
    params = params or {}
    conn = sqlite3.connect(SIGNAL_POOL_DB)
    c = conn.cursor()
    # 只插入未存在且待消费的
    c.execute("""
        SELECT id FROM signals
        WHERE instId=? AND period=? AND signal_type=? AND ts=? AND status='WAIT_SIMU'
    """, (instId, period, signal_type, ts))
    if c.fetchone():
        conn.close()
        return
    c.execute("""
        INSERT INTO signals
        (instId, period, ts, close, vol, signal_type, status, score, params, created_at, source, priority, promotion_level, expire_ts, source_tag)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        instId, period, ts, close, vol, signal_type, 'WAIT_SIMU', score,
        json.dumps(params or {}, ensure_ascii=False), now_ts, source, priority, promotion_level, expire_ts, "signal_engine"
    ))
    conn.commit()
    conn.close()

# =============== 增量K线信号主逻辑 ===============
def fetch_kline_signals():
    total_signal = 0
    now_ts = int(time.time())
    for period in PERIODS:
        window_sec = KLINE_PERIOD_WINDOW.get(period, 90)
        symbols = get_strategy_symbols(period)
        for instId in symbols:
            kline_rows = get_latest_kline(instId, period, window=20)
            if len(kline_rows) < 15: continue
            for signal_type, judge_func in [
                ('BREAKOUT_UP', lambda closes, ma5, ma10, rsi, price: price > ma5 and price > ma10 and rsi and rsi > 70),
                ('BREAKOUT_DOWN', lambda closes, ma5, ma10, rsi, price: price < ma5 and price < ma10 and rsi and rsi < 30)
            ]:
                last_cursor = get_signal_cursor(instId, period, signal_type)
                for ts, price in kline_rows:
                    ts = int(ts)
                    if ts <= last_cursor or ts < now_ts - window_sec: continue
                    closes = [float(x[1]) for x in kline_rows if int(x[0]) <= ts]
                    ma5 = simple_ma(closes, window=5)
                    ma10 = simple_ma(closes, window=10)
                    rsi = simple_rsi(closes, window=14)
                    if ma5 is None or ma10 is None or rsi is None: continue
                    if judge_func(closes, ma5, ma10, rsi, price):
                        insert_signal(instId, period, ts, price, 0.01, signal_type, score=7.5,
                                     params={"ma5": ma5, "ma10": ma10, "rsi": rsi}, source="kline_engine")
                        update_signal_cursor(instId, period, signal_type, ts)
                        total_signal += 1
    print(f"[K线信号] 共产出 {total_signal} 个信号。")
    return total_signal

# ========== 健康巡检：信号表检查 ==========
def signal_health_check():
    conn = sqlite3.connect(SIGNAL_POOL_DB)
    c = conn.cursor()
    now = int(time.time())
    c.execute("SELECT COUNT(*) FROM signals WHERE status='WAIT_SIMU'")
    wait_count = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM signals WHERE status='COLD_START'")
    cold_count = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM signals WHERE status='WAIT_REAL'")
    real_count = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM signals WHERE status='DONE'")
    done_count = c.fetchone()[0]
    c.execute("SELECT MAX(created_at) FROM signals")
    last_signal = c.fetchone()[0]
    print(f"[健康巡检] WAIT_SIMU:{wait_count}, COLD_START:{cold_count}, WAIT_REAL:{real_count}, DONE:{done_count}, 最近信号时间:{last_signal}")
    conn.close()

# =============== 主循环 ===============
def mark_cold_start_signals():
    conn = sqlite3.connect(SIGNAL_POOL_DB)
    c = conn.cursor()
    # 只归档历史信号，不动新信号
    c.execute("UPDATE signals SET status='COLD_START', detected_at=? WHERE status='WAIT_SIMU' AND created_at < ?", (int(time.time()), int(time.time()) - 120))
    conn.commit()
    conn.close()
    print("[冷启动] 已把历史老信号批量标记为COLD_START。")

def main():
    ensure_signal_pool_table()
    print("=== 增量闭环信号生成器 启动 ===")
    cold_start = True
    while True:
        if cold_start:
            mark_cold_start_signals()  # 冷启动信号归档，只归档历史
            cold_start = False
        signal_total = fetch_kline_signals()
        signal_health_check()          # 每轮巡检，实时输出信号池状态
        print(f"\033[94m[信号汇总] 本轮产出 {signal_total} 个信号。\033[0m")
        time.sleep(60)

if __name__ == "__main__":
    main()
