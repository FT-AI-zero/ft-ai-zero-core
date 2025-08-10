import sqlite3
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CANDIDATES = [
    (ROOT/"data"/"live"/"dbs"/"trades.db",      "live_trades"),
    (ROOT/"data"/"paper"/"dbs"/"simu_trades.db","simu_trades"),
]

def ensure_col(db, table, col="action"):
    if not db.exists():
        return
    con = sqlite3.connect(str(db)); cur = con.cursor()
    tb = cur.execute(
        "SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?",[table]
    ).fetchone()[0]
    if not tb:
        con.close(); return
    cols = [r[1] for r in cur.execute(f"PRAGMA table_info({table})")]
    if col not in cols:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {col} TEXT")
        print(f"[ok] add {col} -> {db.name}.{table}")
    con.commit(); con.close()

if __name__ == "__main__":
    for db, table in CANDIDATES:
        ensure_col(db, table)
