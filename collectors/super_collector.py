import os
import time
import datetime
import sqlite3
import requests
import traceback
import json

from core.okx_trader import OKXTrader
from utils.config import DB_DIR
from utils.db_upgrade import ensure_table_fields

FAILED_COLOR = '\033[91m'
OK_COLOR = '\033[92m'
RESET_COLOR = '\033[0m'

def ensure_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)
ensure_dir(DB_DIR)

def load_focus_symbols():
    config_file = os.path.join(DB_DIR, "focus_symbols.json")
    if not os.path.exists(config_file):
        with open(config_file, "w") as f:
            json.dump([
                "BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP","TON-USDT-SWAP",
                "DOGE-USDT-SWAP","XRP-USDT-SWAP","PIU-USDT-SWAP","PUMP-USDT-SWAP"
            ], f)
    with open(config_file, "r") as f:
        return json.load(f)
    
# ---------- 表结构初始化 ----------
def ensure_all_tables():
    # K线（多周期）
    for bar in ["1m", "3m", "5m", "15m", "1H", "4H", "1D"]:
        dbfile = f'kline_{bar}.db'
        tablename = f'kline_{bar}'
        full_path = os.path.join(DB_DIR, dbfile)
        conn = sqlite3.connect(full_path)
        c = conn.cursor()
        c.execute(f'''
            CREATE TABLE IF NOT EXISTS {tablename} (
                instId TEXT, ts INTEGER, open REAL, high REAL, low REAL, close REAL, vol REAL,
                PRIMARY KEY (instId, ts)
            )
        ''')
        ensure_table_fields(full_path, tablename, {
            "instId": "TEXT", "ts": "INTEGER", "open": "REAL", "high": "REAL", "low": "REAL", "close": "REAL", "vol": "REAL"
        })
        conn.commit()
        conn.close()
    # 盘口
    conn = sqlite3.connect(os.path.join(DB_DIR, 'orderbook.db'))
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS orderbook (
            instId TEXT, ts INTEGER,
            bid1 REAL, bid1_qty REAL, ask1 REAL, ask1_qty REAL,
            bids TEXT, asks TEXT,
            PRIMARY KEY (instId, ts)
        )
    ''')
    ensure_table_fields(os.path.join(DB_DIR, 'orderbook.db'), "orderbook", {
        "instId": "TEXT", "ts": "INTEGER", "bid1": "REAL", "bid1_qty": "REAL", "ask1": "REAL", "ask1_qty": "REAL", "bids": "TEXT", "asks": "TEXT"
    })
    conn.commit()
    conn.close()
    # 逐笔成交
    conn = sqlite3.connect(os.path.join(DB_DIR, 'trades.db'))
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS trades (
            instId TEXT, ts INTEGER, px REAL, qty REAL, side TEXT, trade_id TEXT,
            PRIMARY KEY (instId, ts, trade_id)
        )
    ''')
    ensure_table_fields(os.path.join(DB_DIR, 'trades.db'), "trades", {
        "instId": "TEXT", "ts": "INTEGER", "px": "REAL", "qty": "REAL", "side": "TEXT", "trade_id": "TEXT"
    })
    conn.commit()
    conn.close()
    # 资金费率
    conn = sqlite3.connect(os.path.join(DB_DIR, 'funding_rate_8h.db'))
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS funding_rate_8h (
            instId TEXT, ts INTEGER, funding_rate REAL,
            PRIMARY KEY (instId, ts)
        )
    ''')
    ensure_table_fields(os.path.join(DB_DIR, 'funding_rate_8h.db'), "funding_rate_8h", {
        "instId": "TEXT", "ts": "INTEGER", "funding_rate": "REAL"
    })
    conn.commit()
    conn.close()
    # 多空比
    conn = sqlite3.connect(os.path.join(DB_DIR, 'long_short_ratio.db'))
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS long_short_ratio (
            instId TEXT, ts INTEGER, long_short_ratio REAL,
            PRIMARY KEY (instId, ts)
        )
    ''')
    ensure_table_fields(os.path.join(DB_DIR, 'long_short_ratio.db'), "long_short_ratio", {
        "instId": "TEXT", "ts": "INTEGER", "long_short_ratio": "REAL"
    })
    conn.commit()
    conn.close()
    # 爆仓榜
    conn = sqlite3.connect(os.path.join(DB_DIR, 'liquidation.db'))
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS liquidation (
            instId TEXT, ts INTEGER, px REAL, sz REAL, side TEXT,
            PRIMARY KEY(instId, ts)
        )
    ''')
    ensure_table_fields(os.path.join(DB_DIR, 'liquidation.db'), "liquidation", {
        "instId": "TEXT", "ts": "INTEGER", "px": "REAL", "sz": "REAL", "side": "TEXT"
    })
    conn.commit()
    conn.close()

# ---------- 数据入库 ----------
def save_kline_to_db(instId, bar, klines):
    dbfile = f'kline_{bar}.db'
    tablename = f'kline_{bar}'
    full_path = os.path.join(DB_DIR, dbfile)
    conn = sqlite3.connect(full_path)
    c = conn.cursor()
    for item in klines:
        try:
            ts = int(int(item[0]) // 1000)
            o, h, l, c_, v = map(float, item[1:6])
            c.execute(f'''
                INSERT OR IGNORE INTO {tablename} (instId, ts, open, high, low, close, vol)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (instId, ts, o, h, l, c_, v))
        except Exception as e:
            print(f"{FAILED_COLOR}[KLINE][{bar}] 写入异常 {instId}: {e}{RESET_COLOR}")
    conn.commit()
    conn.close()

def save_orderbook_to_db(instId, ob):
    dbfile = 'orderbook.db'
    tablename = 'orderbook'
    full_path = os.path.join(DB_DIR, dbfile)
    conn = sqlite3.connect(full_path)
    c = conn.cursor()
    for item in ob:
        try:
            ts = int(int(item.get("ts", time.time()*1000)) // 1000)
            bids = item.get("bids", [])
            asks = item.get("asks", [])
            bid1, bid1_qty = (float(bids[0][0]), float(bids[0][1])) if bids else (0, 0)
            ask1, ask1_qty = (float(asks[0][0]), float(asks[0][1])) if asks else (0, 0)
            c.execute(f'''
                INSERT OR IGNORE INTO {tablename} (instId, ts, bid1, bid1_qty, ask1, ask1_qty, bids, asks)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                instId, ts, bid1, bid1_qty, ask1, ask1_qty,
                json.dumps(bids), json.dumps(asks)
            ))
        except Exception as e:
            print(f"{FAILED_COLOR}[ORDERBOOK][{instId}]写入异常: {e}{RESET_COLOR}")
    conn.commit()
    conn.close()

def save_trades_to_db(instId, trades):
    dbfile = 'trades.db'
    tablename = 'trades'
    full_path = os.path.join(DB_DIR, dbfile)
    conn = sqlite3.connect(full_path)
    c = conn.cursor()
    for t in trades:
        try:
            ts = int(int(t.get("ts", time.time()*1000)) // 1000)
            px = float(t["px"])
            qty = float(t["sz"])
            side = t["side"]
            trade_id = t.get("tradeId", t.get("id", str(ts)))
            c.execute(f'''
                INSERT OR IGNORE INTO {tablename} (instId, ts, px, qty, side, trade_id)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (instId, ts, px, qty, side, trade_id))
        except Exception as e:
            print(f"{FAILED_COLOR}[TRADES][{instId}]写入异常: {e}{RESET_COLOR}")
    conn.commit()
    conn.close()

def save_funding_rate(instId, data):
    dbfile = 'funding_rate_8h.db'
    tablename = 'funding_rate_8h'
    full_path = os.path.join(DB_DIR, dbfile)
    conn = sqlite3.connect(full_path)
    c = conn.cursor()
    try:
        ts = int(data.get("fundingTime", int(time.time()*1000))) // 1000
        rate = float(data["fundingRate"])
        c.execute(f'''
            INSERT OR IGNORE INTO {tablename} (instId, ts, funding_rate)
            VALUES (?, ?, ?)
        ''', (instId, ts, rate))
    except Exception as e:
        print(f"{FAILED_COLOR}[FUNDING_RATE]{instId}写入异常: {e}{RESET_COLOR}")
    conn.commit()
    conn.close()

def save_long_short_ratio(instId, data):
    dbfile = 'long_short_ratio.db'
    tablename = 'long_short_ratio'
    full_path = os.path.join(DB_DIR, dbfile)
    conn = sqlite3.connect(full_path)
    c = conn.cursor()
    try:
        d = data[0] if data else {}
        ts = int(d.get("ts", int(time.time()*1000))) // 1000
        ratio = float(d.get("longShortRatio", 0))
        c.execute(f'''
            INSERT OR IGNORE INTO {tablename} (instId, ts, long_short_ratio)
            VALUES (?, ?, ?)
        ''', (instId, ts, ratio))
    except Exception as e:
        print(f"{FAILED_COLOR}[LSRATIO]{instId}写入异常: {e}{RESET_COLOR}")
    conn.commit()
    conn.close()

def save_liquidation_to_db(instId, data):
    dbfile = "liquidation.db"
    tablename = "liquidation"
    full_path = os.path.join(DB_DIR, dbfile)
    conn = sqlite3.connect(full_path)
    c = conn.cursor()
    for d in data:
        try:
            c.execute(
                f'INSERT OR IGNORE INTO {tablename} (instId, ts, px, sz, side) VALUES (?, ?, ?, ?, ?)',
                (d["instId"], int(d["ts"]), float(d["px"]), float(d["sz"]), d["side"])
            )
        except Exception as e:
            print(f"{FAILED_COLOR}[写入爆仓榜失败]{e}{RESET_COLOR}")
    conn.commit()
    conn.close()

def safe_request(func, max_retry=5, *args, **kwargs):
    for i in range(max_retry):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            print(f"{FAILED_COLOR}[重试{i+1}/{max_retry}] {func.__name__} 异常: {e}{RESET_COLOR}")
            time.sleep(2)
    print(f"{FAILED_COLOR}[致命] {func.__name__} 多次重试失败！{RESET_COLOR}")
    return None

def check_and_fill_kline_gap(trader, instId, bar, days=2, max_gap=15):
    dbfile = f'kline_{bar}.db'
    tablename = f'kline_{bar}'
    full_path = os.path.join(DB_DIR, dbfile)
    now = int(time.time())
    begin_ts = now - days * 86400
    conn = sqlite3.connect(full_path)
    c = conn.cursor()
    c.execute(f"SELECT ts FROM {tablename} WHERE instId=? AND ts>=? ORDER BY ts", (instId, begin_ts))
    rows = c.fetchall()
    conn.close()
    ts_set = set([r[0] for r in rows])
    bar_sec = {"1m": 60, "3m":180, "5m":300, "15m":900, "1H":3600, "4H":14400, "1D":86400}.get(bar, 60)
    miss_ts = []
    for t in range(begin_ts, now, bar_sec):
        if t not in ts_set:
            miss_ts.append(t)
    if miss_ts:
        print(f"{FAILED_COLOR}[闭环][{instId}][{bar}] 检测到{len(miss_ts)}个缺口，自动补齐...{RESET_COLOR}")
        for t in miss_ts[:max_gap]:
            try:
                klines = safe_request(trader.get_kline, 5, instId, bar=bar, limit=1, after=t*1000)
                if klines:
                    save_kline_to_db(instId, bar, klines)
            except Exception as e:
                print(f"{FAILED_COLOR}[闭环补采][{instId}][{bar}][{t}]异常: {e}{RESET_COLOR}")
        print(f"{OK_COLOR}[闭环][{instId}][{bar}] 缺口已尝试补齐{RESET_COLOR}")
    else:
        print(f"{OK_COLOR}[闭环][{instId}][{bar}]无缺口，健康{RESET_COLOR}")

def main():
    print("===== 超级多源行情采集器启动（K线/盘口/资金/爆仓/多空/费率 全闭环） =====")
    ensure_dir(DB_DIR)
    ensure_all_tables()
    trader = OKXTrader()
    periods = ["1m", "3m", "5m", "15m", "1H", "4H", "1D"]
    round_count = 0

    while True:
        round_count += 1
        insts = trader.get_all_instruments("SWAP")
        all_inst_ids = [x['instId'] for x in insts]  # <== 恢复全币种
        print(f"\n采集轮次: {round_count}，合约品种: 共{len(all_inst_ids)}个")

        n_kline, n_ob, n_trades, n_fund, n_lsr, n_liq, n_fail = 0,0,0,0,0,0,0
        t0 = time.time()

        for instId in all_inst_ids:
            # K线
            for bar in periods:
                try:
                    klines = safe_request(trader.get_kline, 5, instId, bar=bar, limit=100)
                    if klines:
                        save_kline_to_db(instId, bar, klines)
                        n_kline += len(klines)
                    check_and_fill_kline_gap(trader, instId, bar, days=2, max_gap=15)
                except Exception as e:
                    print(f"{FAILED_COLOR}[KLINE][{bar}] {instId} 采集异常: {e}{RESET_COLOR}")
                    n_fail += 1
                time.sleep(0.07)
            # 盘口
            try:
                ob = safe_request(trader.get_orderbook, 3, instId)
                if ob:
                    save_orderbook_to_db(instId, ob)
                    n_ob += 1
            except Exception as e:
                print(f"{FAILED_COLOR}[ORDERBOOK]{instId}采集异常:{e}{RESET_COLOR}")
                n_fail += 1
            # 逐笔成交
            try:
                trades = safe_request(trader.get_trades, 3, instId, limit=50)
                if trades:
                    save_trades_to_db(instId, trades)
                    n_trades += len(trades)
            except Exception as e:
                print(f"{FAILED_COLOR}[TRADES]{instId}采集异常:{e}{RESET_COLOR}")
                n_fail += 1
            # 资金费率
            try:
                fund = trader.get_funding_rate(instId)
                if fund and fund.get("fundingRate") is not None:
                    save_funding_rate(instId, fund)
                    n_fund += 1
            except Exception as e:
                print(f"{FAILED_COLOR}[FUND]{instId}采集异常:{e}{RESET_COLOR}")
                n_fail += 1
            # 多空比
            try:
                lsr = trader.get_long_short_ratio(instId)
                if lsr and isinstance(lsr, list) and len(lsr) > 0:
                    save_long_short_ratio(instId, lsr)
                    n_lsr += 1
            except Exception as e:
                print(f"{FAILED_COLOR}[LSR]{instId}采集异常:{e}{RESET_COLOR}")
                n_fail += 1
            # 爆仓榜
            try:
                liq = trader.get_liquidation(instId, limit=20)
                if liq:
                    save_liquidation_to_db(instId, liq)
                    n_liq += len(liq)
            except Exception as e:
                print(f"{FAILED_COLOR}[LIQ]{instId}采集异常:{e}{RESET_COLOR}")
                n_fail += 1

        print(f"\n本轮采集完成，K线:{n_kline}，盘口:{n_ob}，成交:{n_trades}，资金费率:{n_fund}，多空比:{n_lsr}，爆仓:{n_liq}，异常:{n_fail}，耗时:{int(time.time()-t0)}秒")
        print("休息60秒 ...")
        time.sleep(60)

if __name__ == "__main__":
    main()
