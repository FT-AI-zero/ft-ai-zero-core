import sqlite3
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PAPER_DB = ROOT / "data" / "paper" / "dbs" / "strategy_pool.db"
LIVE_DB  = ROOT / "data" / "live"  / "dbs" / "strategy_pool.db"

DDL = """
CREATE TABLE IF NOT EXISTS allowlist(
  param_group_id INTEGER,
  window TEXT,
  score REAL,
  trades INTEGER,
  source TEXT,
  weight REAL DEFAULT 1.0,
  updated_at TEXT
);
"""
IDX = "CREATE UNIQUE INDEX IF NOT EXISTS ux_allowlist_pg_win ON allowlist(param_group_id, window);"

def touch(db):
    db.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(db)); cur = con.cursor()
    cur.executescript(DDL); cur.execute(IDX)
    cols = [r[1] for r in cur.execute("PRAGMA table_info(allowlist)")]
    if "interval" not in cols:
        cur.execute("ALTER TABLE allowlist ADD COLUMN interval TEXT")
        cur.execute("UPDATE allowlist SET interval = window WHERE interval IS NULL")
    con.commit(); con.close()
    print(f"[ok] allowlist.interval ready -> {db}")

if __name__ == "__main__":
    for db in (PAPER_DB, LIVE_DB):
        touch(db)
