# -*- coding: utf-8 -*-
# tools/migrate_review_schema.py
import os, sys, sqlite3

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path: sys.path.insert(0, ROOT)

from utils.config import DB_DIR

REVIEW_DB = os.path.join(DB_DIR, "review.db")

SQLS = [
    # 成交K线缓存（避免重复拉OKX）
    """
    CREATE TABLE IF NOT EXISTS kline_cache (
        instId TEXT NOT NULL,
        bar    TEXT NOT NULL,
        ts     INTEGER NOT NULL,   -- 秒级UTC, 该K线的开始时间
        open   REAL, high REAL, low REAL, close REAL, vol REAL,
        PRIMARY KEY(instId, bar, ts)
    )
    """,
    # 真PnL回放（逐笔开仓的退出、收益、风险统计）
    """
    CREATE TABLE IF NOT EXISTS pnl_by_trade (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        live_trade_id INTEGER,       -- 对应 review.live_trades.id
        instId TEXT, gid INTEGER, side TEXT,
        open_ts TEXT, close_ts TEXT,
        entry REAL, exit REAL, qty REAL,
        hold_sec INTEGER,
        tp REAL, sl REAL, trail_ratio REAL,
        exit_reason TEXT,
        taker_fee_rate REAL, fees REAL,
        pnl REAL, pnl_pct REAL,
        mfe REAL, mae REAL,           -- 按 1m 轨迹计算
        created_at TEXT DEFAULT (datetime('now')),
        UNIQUE(live_trade_id)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_pnl_gid ON pnl_by_trade(gid)",
    "CREATE INDEX IF NOT EXISTS idx_pnl_open ON pnl_by_trade(open_ts)",
    "CREATE INDEX IF NOT EXISTS idx_pnl_close ON pnl_by_trade(close_ts)"
]

def main():
    os.makedirs(DB_DIR, exist_ok=True)
    conn = sqlite3.connect(REVIEW_DB)
    for sql in SQLS:
        conn.execute(sql)
    conn.commit(); conn.close()
    print("[migrate] review schema ready:", REVIEW_DB)

if __name__ == "__main__":
    main()
