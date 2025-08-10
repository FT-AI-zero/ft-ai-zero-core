# tools/migrate_sp_allowlist.py
import os, sqlite3
from pathlib import Path

def pick_sp_db():
    ROOT = Path(__file__).resolve().parents[1]
    cands = [
        ROOT / "data" / "paper" / "dbs" / "strategy_pool.db",
        ROOT / "data" / "live"  / "dbs" / "strategy_pool.db",
        ROOT / "data" / "dbs"   / "strategy_pool.db",
    ]
    for p in cands:
        if p.exists():
            return str(p)
    p = cands[0]
    p.parent.mkdir(parents=True, exist_ok=True)
    return str(p)

def ensure_column(cur, table, col, type_):
    cur.execute(f"PRAGMA table_info({table})")
    cols = {r[1] for r in cur.fetchall()}
    if col not in cols:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {col} {type_}")

def main():
    sp_db = pick_sp_db()
    conn = sqlite3.connect(sp_db)
    cur  = conn.cursor()

    # 建表（若不存在）
    cur.execute("""
    CREATE TABLE IF NOT EXISTS allowlist(
      param_group_id INTEGER,
      window TEXT,
      score REAL,
      trades INTEGER,
      source TEXT,
      weight REAL DEFAULT 1.0,
      updated_at TEXT
    );
    """)

    # 旧库补列
    ensure_column(cur, "allowlist", "weight",    "REAL DEFAULT 1.0")
    ensure_column(cur, "allowlist", "updated_at","TEXT")

    # ★ 关键：两个唯一索引都建上，适配不同 upsert 代码
    cur.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS ux_allowlist_pg_win
    ON allowlist(param_group_id, window);
    """)
    cur.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS ux_allowlist_pg_win_src
    ON allowlist(param_group_id, window, source);
    """)

    conn.commit(); conn.close()
    print(f"[migrate] allowlist schema OK -> {sp_db}")

if __name__ == "__main__":
    main()
