# tools/quick_fix_after_start.py
import sqlite3, os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PAPER_DB  = ROOT / "data" / "paper"  / "dbs" / "strategy_pool.db"
LIVE_DB   = ROOT / "data" / "live"   / "dbs" / "strategy_pool.db"
SIG_SHARED= ROOT / "data" / "shared" / "dbs" / "signals.db"
SIG_PAPER = ROOT / "data" / "paper"  / "dbs" / "signals.db"

def ensure_allowlist(db_path: Path):
    if not db_path.exists():
        db_path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(db_path)); cur = con.cursor()
    # 建表（兼容我们现在用到的字段）
    cur.execute("""
        CREATE TABLE IF NOT EXISTS allowlist(
          param_group_id INTEGER,
          window TEXT,
          score REAL,
          trades INTEGER,
          source TEXT,
          weight REAL DEFAULT 1.0,
          updated_at TEXT
        )
    """)
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_allowlist_pg_win ON allowlist(param_group_id, window)")
    # 别名列 interval（兼容旧代码）
    cols = [r[1] for r in cur.execute("PRAGMA table_info(allowlist)")]
    if "interval" not in cols:
        cur.execute("ALTER TABLE allowlist ADD COLUMN interval TEXT")
        cur.execute("UPDATE allowlist SET interval = window WHERE interval IS NULL")
    con.commit(); con.close()

def ensure_signals(db_path: Path):
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(db_path)); cur = con.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS signals(id INTEGER PRIMARY KEY, status TEXT, created_at INTEGER)")
    cols = [r[1] for r in cur.execute("PRAGMA table_info(signals)")]
    if "detected_at" not in cols:
        cur.execute("ALTER TABLE signals ADD COLUMN detected_at INTEGER")
    con.commit(); con.close()

def main():
    for db in [PAPER_DB, LIVE_DB]:
        ensure_allowlist(db)
    for db in [SIG_SHARED, SIG_PAPER]:
        ensure_signals(db)
    print("[ok] quick_fix_after_start done.")

if __name__ == "__main__":
    main()
