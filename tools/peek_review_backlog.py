# -*- coding: utf-8 -*-
import os, sys, sqlite3, datetime

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path: sys.path.insert(0, ROOT)

from utils.config import DB_DIR

REVIEW_DB = os.path.join(DB_DIR, "review.db")

def open_db(path):
    conn = sqlite3.connect(path, timeout=30.0, isolation_level=None)
    try:
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
    except Exception:
        pass
    return conn

def main():
    c = open_db(REVIEW_DB)
    q = c.execute("""
        SELECT COUNT(*)
        FROM live_trades t
        LEFT JOIN pnl_by_trade p ON p.live_trade_id = t.id
        WHERE p.id IS NULL
    """).fetchone()[0]
    print(f"backlog (un-replayed live_trades) = {q}")

    print("\n== latest 10 un-replayed ==")
    rows = c.execute("""
        SELECT t.id, t.instId, t.side, t.price, t.vol, t.gid, t.ts
        FROM live_trades t
        LEFT JOIN pnl_by_trade p ON p.live_trade_id = t.id
        WHERE p.id IS NULL
        ORDER BY t.id DESC
        LIMIT 10
    """).fetchall()
    for r in rows:
        print("  ", r)

    total_pnl = c.execute("SELECT COUNT(*) FROM pnl_by_trade").fetchone()[0]
    print(f"\n== pnl_by_trade rows = {total_pnl}")
    c.close()

if __name__ == "__main__":
    main()
