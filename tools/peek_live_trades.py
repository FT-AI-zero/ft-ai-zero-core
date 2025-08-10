import os, sys, sqlite3
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from utils.config import REVIEW_DB

def main():
    conn = sqlite3.connect(REVIEW_DB)
    rows = conn.execute("""
        SELECT id, instId, side, price, vol, gid, ts
        FROM live_trades
        ORDER BY ts DESC
        LIMIT 20
    """).fetchall()
    conn.close()

    print("live_trades (latest 20):")
    if not rows:
        print("  <empty>")
        return
    for r in rows:
        print(" ", r)

if __name__ == "__main__":
    main()
