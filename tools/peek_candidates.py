import os, sys, sqlite3, datetime as dt
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from utils.config import AI_PARAMS_DB

def main():
    today = dt.date.today().isoformat()
    conn = sqlite3.connect(AI_PARAMS_DB)
    rows = conn.execute("""
        SELECT param_group_id, window, trades, score, source, created_at
        FROM candidates
        WHERE created_at = ?
        ORDER BY score DESC, trades DESC
        LIMIT 50
    """, (today,)).fetchall()
    conn.close()

    print("candidates (today):")
    if not rows:
        print("  <empty>")
        return
    for r in rows:
        print(" ", r)

if __name__ == "__main__":
    main()
