# core/trade_engine.py
from __future__ import annotations
import os
import sqlite3
import threading
import time
import datetime
import json
import traceback

# ✅ 包内相对导入，不要再 from okx_trader import ...
from .gateway import OkxGateway, PaperGateway

# ✅ 用 utils.config 里的配置常量
from utils.config import (
    MODE,                     # "live" 或 "paper"
    TRADE_ENGINE_LOG,
    HEALTH_LOG,
    SIGNAL_POOL_DB,
    SIMU_TRADES_DB,
)

# ✅ 你的 AI 决策/演化接口
from ailearning.ai_engine import ai_risk_decision, ai_evolution, load_ai_pool

FAILED_COLOR = '\033[91m'
OK_COLOR = '\033[92m'
RESET_COLOR = '\033[0m'
ORDER_LOCK = threading.Lock()
HEARTBEAT_INTERVAL = 60

# 明确只做模拟盘：网关取 PaperGateway（若 MODE == "live" 可切 OkxGateway）
def make_gateway():
    return PaperGateway() if MODE != "live" else OkxGateway()

def safe_float(v, default=0.0):
    try:
        return float(v)
    except Exception:
        return default

def parse_ts(ts):
    if isinstance(ts, (int, float)):
        return int(ts)
    if isinstance(ts, str):
        try:
            return int(float(ts))
        except Exception:
            try:
                return int(datetime.datetime.fromisoformat(ts).timestamp())
            except Exception:
                return int(time.time())
    return int(time.time())

def get_trades_db():
    return SIMU_TRADES_DB

def write_health_status(status="OK", msg=""):
    try:
        os.makedirs(os.path.dirname(HEALTH_LOG), exist_ok=True)
        with open(HEALTH_LOG, "a", encoding="utf-8") as f:
            f.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] trade_engine.py {status} {msg}\n")
    except Exception as e:
        print(f"{FAILED_COLOR}[健康日志写入异常] {e}{RESET_COLOR}")

def write_heartbeat():
    try:
        os.makedirs(os.path.dirname(TRADE_ENGINE_LOG), exist_ok=True)
        with open(TRADE_ENGINE_LOG, "w", encoding="utf-8") as f:
            f.write(f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} OK trade_engine_simulator running\n")
    except Exception as e:
        print(f"{FAILED_COLOR}[心跳日志写入异常] {e}{RESET_COLOR}")

def ensure_tables():
    # simu_trades 表
    conn = sqlite3.connect(get_trades_db())
    conn.execute('''
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            instId TEXT,
            ts INTEGER,
            price REAL,
            vol REAL,
            status TEXT,
            strategy_id TEXT,
            comment TEXT,
            param_group_id INTEGER,
            meta TEXT
        )
    ''')
    conn.commit()
    conn.close()

    # signals 表
    conn = sqlite3.connect(SIGNAL_POOL_DB)
    conn.execute('''
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            instId TEXT,
            interval TEXT,
            ts INTEGER,
            close REAL,
            vol REAL,
            signal_type TEXT,
            status TEXT,
            detected_at TEXT,
            priority INTEGER,
            promotion_level INTEGER,
            expire_ts INTEGER,
            meta TEXT
        )
    ''')
    conn.commit()
    conn.close()

def save_trade(trade, is_open=True, side='long'):
    with ORDER_LOCK:
        try:
            comment = "open_" + side if is_open else "close_" + side
            param_group_id = trade.get("param_group_id")
            conn = sqlite3.connect(get_trades_db())
            conn.execute('''
                INSERT INTO trades(instId, ts, price, vol, status, strategy_id, comment, param_group_id, meta)
                VALUES(?,?,?,?,?,?,?,?,?)
            ''', (
                trade.get("instId", ""),
                parse_ts(trade.get("ts")),
                safe_float(trade.get("price")),
                safe_float(trade.get("vol")),
                trade.get("status", ""),
                trade.get("strategy_id", ""),
                comment,
                param_group_id,
                json.dumps(trade.get("meta", {}), ensure_ascii=False)
            ))
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"{FAILED_COLOR}[存储订单失败] {e}{RESET_COLOR}")
            write_health_status("ERROR", f"save_trade: {e}")

def save_signal(signal, status="FILLED"):
    try:
        conn = sqlite3.connect(SIGNAL_POOL_DB)
        conn.execute('''
            INSERT INTO signals(instId, interval, ts, close, vol, signal_type, status, detected_at, meta)
            VALUES(?,?,?,?,?,?,?,?,?)
        ''', (
            signal.get("instId", ""),
            signal.get("interval", ""),
            parse_ts(signal.get("ts")),
            safe_float(signal.get("close")),
            safe_float(signal.get("vol")),
            signal.get("signal_type", ""),
            status,
            datetime.datetime.utcnow().isoformat(),
            json.dumps(signal.get("meta", {}), ensure_ascii=False)
        ))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"{FAILED_COLOR}[存储信号失败] {e}{RESET_COLOR}")
        write_health_status("ERROR", f"save_signal: {e}")

def fetch_waiting_signals():
    try:
        now_ts = int(time.time())
        conn = sqlite3.connect(SIGNAL_POOL_DB)
        rows = conn.execute('''
            SELECT id, instId, interval, ts, close, vol, signal_type, meta,
                   IFNULL(priority, 3), IFNULL(promotion_level, 0), IFNULL(expire_ts, 0)
            FROM signals
            WHERE status='WAIT_SIMU'
              AND (expire_ts IS NULL OR expire_ts=0 OR expire_ts > ?)
            ORDER BY priority ASC, promotion_level DESC, ts ASC
        ''', (now_ts,)).fetchall()
        conn.close()
        signals = []
        for r in rows:
            sid, inst, interval, ts, close, vol, sigtype, meta, priority, promotion_level, expire_ts = r
            try:
                m = json.loads(meta) if meta else {}
            except Exception:
                m = {}
            signals.append({
                'id': sid,
                'instId': inst,
                'interval': interval,
                'ts': ts,
                'close': close,
                'vol': vol,
                'signal_type': sigtype,
                'meta': m,
                'priority': priority,
                'promotion_level': promotion_level,
                'expire_ts': expire_ts
            })
        return signals
    except Exception as e:
        print(f"{FAILED_COLOR}[读取信号失败] {e}{RESET_COLOR}")
        write_health_status("ERROR", f"fetch_waiting_signals: {e}")
        return []

def mark_signal_done(sid, status="DONE"):
    try:
        conn = sqlite3.connect(SIGNAL_POOL_DB)
        conn.execute(
            "UPDATE signals SET status=?, detected_at=? WHERE id=?",
            (status, datetime.datetime.utcnow().isoformat(), sid)
        )
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"{FAILED_COLOR}[更新信号状态失败] {e}{RESET_COLOR}")
        write_health_status("ERROR", f"mark_signal_done: {e}")

def heartbeat_thread():
    while True:
        try:
            write_heartbeat()
        except Exception as e:
            print(f"{FAILED_COLOR}[心跳异常] {e}{RESET_COLOR}")
            write_health_status('ERROR', f"heartbeat_thread: {e}")
        time.sleep(HEARTBEAT_INTERVAL)

def main():
    print("===== 模拟盘交易引擎启动 =====")
    ensure_tables()
    threading.Thread(target=heartbeat_thread, daemon=True).start()

    gateway = make_gateway()  # ✅ 网关在这里实例化
    counter = 0
    reject_reason_stat = {}

    while True:
        write_health_status('OK')
        signals = fetch_waiting_signals()
        if not signals:
            time.sleep(5)
            continue

        ai_param_groups = load_ai_pool(min_win_rate=0, min_score=0, top_k=10)

        for sig in signals:
            for group in ai_param_groups:
                params = group.get("params", {})
                meta = sig.get('meta') or {}
                full_params = {**params, **meta.get('params', {})}
                full_params['instId'] = sig['instId']
                full_params['side']   = meta.get('side') or sig.get('signal_type','').lower()
                full_params['last_price'] = sig['close']
                full_params['avail_pos']  = sig['vol']
                sig['param_group_id'] = group.get('id')

                result = ai_risk_decision(sig, params=full_params, mode='open')
                if result.get('pass'):
                    # 这里只模拟入库，不真实下单；实盘切 OkxGateway 后在这里调用 gateway.open_market
                    save_trade({
                        'instId': sig['instId'],
                        'ts': sig['ts'],
                        'price': sig['close'],
                        'vol': sig['vol'],
                        'status': 'FILLED',
                        'strategy_id': meta.get('strategy_id',''),
                        'param_group_id': group.get('id'),
                        'comment': full_params['side'],
                        'meta': {**meta, 'param_group_id': group.get('id')}
                    }, is_open=True, side=full_params['side'])
                else:
                    reason = result.get('reason') or '未知'
                    reject_reason_stat[reason] = reject_reason_stat.get(reason, 0) + 1

            mark_signal_done(sig['id'], 'DONE')

        counter += 1
        if counter % 5 == 0:
            print("=== 风控拒绝原因统计（全局累计） ===")
            if not reject_reason_stat:
                print("暂无风控拒绝发生")
            else:
                for reason, n in reject_reason_stat.items():
                    print(f"{reason} : {n} 次")

        if counter % 20 == 0:
            ai_evolution()

        time.sleep(2)

if __name__ == '__main__':
    main()
