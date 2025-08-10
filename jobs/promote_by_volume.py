# jobs/promote_by_volume.py
import sqlite3, time, datetime as dt
from utils.config import AI_PARAMS_DB
MIN_TRADES = 2   # 先放宽，保证能晋升一批看链路

def q(c, sql, args=()):
    return c.execute(sql, args).fetchall()

def main():
    print("[DB] AI:", AI_PARAMS_DB)
    conn = sqlite3.connect(AI_PARAMS_DB)
    c = conn.cursor()

    # 1) 准备 candidates 表
    c.execute("""
    CREATE TABLE IF NOT EXISTS candidates(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        param_group_id INTEGER,
        window TEXT,
        trades INTEGER,
        win_rate REAL,
        score REAL,
        created_at TEXT DEFAULT (datetime('now'))
    )
    """)
    # 唯一性（同一 gid+window 当天只插一次）
    c.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS ix_cand_unique
    ON candidates(param_group_id, window, created_at)
    """)
    conn.commit()

    # 2) 选出近7天快照中成交数>=阈值的分组（这里不用 win_rate 了）
    rows = q(conn, """
        SELECT param_group_id, window, trades, win_rate, score
        FROM ai_snapshots
        WHERE window='7d' AND trades>=?
        ORDER BY score DESC, trades DESC
        LIMIT 20
    """, (MIN_TRADES,))

    if not rows:
        print("[PROMOTED] no candidates met the (volume-only) rule.")
        return

    now = dt.datetime.utcnow().date().isoformat()
    promoted = 0
    for gid, window, trades, win_rate, score in rows:
        try:
            conn.execute("""
                INSERT OR IGNORE INTO candidates(param_group_id, window, trades, win_rate, score, created_at)
                VALUES(?,?,?,?,?,?)
            """, (gid, window, trades, win_rate, score, now))
            promoted += conn.total_changes
        except Exception as e:
            print("[WARN] insert candidate failed:", e)
    conn.commit()

    # 3) 打印候选
    rows2 = q(conn, "SELECT param_group_id, window, trades, win_rate, score, created_at FROM candidates ORDER BY id DESC LIMIT 20")
    print(f"[PROMOTED] {promoted} new rows. Top candidates:")
    for r in rows2:
        print(" ", r)

    conn.close()

if __name__ == "__main__":
    main()
