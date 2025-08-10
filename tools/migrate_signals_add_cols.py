# tools/migrate_signals_add_cols.py
import sqlite3
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DBS = [
    ROOT / "data" / "paper" / "dbs" / "signals.db",
    ROOT / "data" / "live"  / "dbs" / "signals.db",
]

def ensure_col(cur, table, col, ddl):
    cols = {r[1] for r in cur.execute(f"PRAGMA table_info({table})")}
    if col not in cols:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {ddl}")
        return True
    return False

def main():
    for db in DBS:
        if not db.exists():
            print(f"[skip] {db} (not exists)"); continue
        con = sqlite3.connect(str(db)); cur = con.cursor()
        try:
            added1 = ensure_col(cur, "signals", "detected_at", "TEXT")
            added2 = ensure_col(cur, "signals", "interval",    "TEXT")
            con.commit()
            print(f"[ok] signals migrated -> {db} (detected_at:{added1}, interval:{added2})")
        except Exception as e:
            print(f"[err] {db}: {e}")
        finally:
            con.close()

if __name__ == "__main__":
    main()
