# tools/peek_allowlist.py
import os, sys, sqlite3
from pathlib import Path

# 保证项目根在 sys.path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# 选库（paper 优先）
def pick_sp_db():
    cands = [
        ROOT / "data" / "paper" / "dbs" / "strategy_pool.db",
        ROOT / "data" / "live"  / "dbs" / "strategy_pool.db",
        ROOT / "data" / "dbs"   / "strategy_pool.db",
    ]
    for p in cands:
        if p.exists():
            return str(p)
    return str(cands[0])

def main():
    sp_db = pick_sp_db()
    conn = sqlite3.connect(sp_db)
    cur = conn.cursor()

    cur.execute("PRAGMA table_info(allowlist)")
    cols = {r[1] for r in cur.fetchall()}
    if not cols:
        print("allowlist table not found."); return

    base = ["param_group_id","window","score","trades","source"]
    extra = []
    if "weight" in cols: extra.append("weight")
    if "updated_at" in cols: extra.append("updated_at")
    if "created_at" in cols and "updated_at" not in cols:
        extra.append("created_at")

    sel = ", ".join(base + extra)
    order = "ORDER BY (score*COALESCE(weight,1.0)) DESC"
    if "updated_at" in cols:
        order += ", updated_at DESC"

    rows = cur.execute(f"SELECT {sel} FROM allowlist {order} LIMIT 20").fetchall()
    conn.close()

    print("allowlist TOP20:")
    for r in rows:
        print(r)

if __name__ == "__main__":
    main()
