# tools/fill_allow_weight.py
import os, sys, sqlite3

# 让脚本无论从哪里运行都能找到工程根的 utils
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from utils.config import STRATEGY_POOL_DB

def main():
    conn = sqlite3.connect(STRATEGY_POOL_DB)
    cur = conn.cursor()
    cur.execute("UPDATE allowlist SET weight=1.0 WHERE weight IS NULL")
    conn.commit()
    print("[fix] filled NULL weights -> 1.0")
    conn.close()

if __name__ == "__main__":
    main()
