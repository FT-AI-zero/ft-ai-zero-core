# tools/peek_pm_exp.py
import os, sys, sqlite3

# ---- 引导 sys.path 到项目根 ----
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from utils.config import DB_DIR

p = os.path.join(DB_DIR, "pm_experience.db")
if not os.path.exists(p):
    print("pm_experience.db not found:", p)
    raise SystemExit(0)

con = sqlite3.connect(p); cur = con.cursor()
rows = cur.execute("""
SELECT id,ts,instId,
       substr(state_json,1,60),
       substr(action_json,1,60),
       reward
FROM experiences
ORDER BY id DESC
LIMIT 10
""").fetchall()
con.close()

print("pm_experience (latest 10):")
for r in rows:
    print(r)
