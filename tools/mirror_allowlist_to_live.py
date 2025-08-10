# tools/mirror_allowlist_to_live.py
import sqlite3
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parents[1]
PAPER = ROOT / "data" / "paper" / "dbs" / "strategy_pool.db"
LIVE  = ROOT / "data" / "live"  / "dbs" / "strategy_pool.db"

DDL = """
CREATE TABLE IF NOT EXISTS allowlist(
  param_group_id INTEGER,
  window TEXT,
  score REAL,
  trades INTEGER,
  source TEXT,
  weight REAL DEFAULT 1.0,
  updated_at TEXT,
  interval TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_allowlist_pg_win ON allowlist(param_group_id, window);
"""

def main():
    LIVE.parent.mkdir(parents=True, exist_ok=True)
    src = sqlite3.connect(str(PAPER)); s = src.cursor()
    dst = sqlite3.connect(str(LIVE));  d = dst.cursor()
    d.executescript(DDL)

    rows = s.execute("""
        SELECT param_group_id, window, score, trades, source, COALESCE(weight,1.0), updated_at
        FROM allowlist
    """).fetchall()

    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    up = 0
    for pg, win, score, trades, source, weight, updated_at in rows:
        interval = win  # interval 与 window 保持一致
        d.execute("""
            INSERT INTO allowlist(param_group_id, window, score, trades, source, weight, updated_at, interval)
            VALUES(?,?,?,?,?,?,?,?)
            ON CONFLICT(param_group_id, window) DO UPDATE SET
              score=excluded.score,
              trades=excluded.trades,
              source=excluded.source,
              weight=excluded.weight,
              updated_at=excluded.updated_at,
              interval=excluded.interval
        """, (pg, win, score, trades, source, weight, updated_at or now, interval))
        up += 1

    dst.commit()
    src.close(); dst.close()
    print(f"[mirror] copied {up} rows -> {LIVE}")

if __name__ == "__main__":
    main()
