# -*- coding: utf-8 -*-
import os, sys, sqlite3

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
    conn = open_db(REVIEW_DB)
    cur = conn.cursor()
    n = cur.execute("SELECT COUNT(*) FROM pnl_by_trade WHERE exit_reason='no_kline'").fetchone()[0]
    cur.execute("DELETE FROM pnl_by_trade WHERE exit_reason='no_kline'")
    conn.commit()
    print(f"[clean] deleted no_kline rows: {n}")
    conn.close()

if __name__ == "__main__":
    main()
