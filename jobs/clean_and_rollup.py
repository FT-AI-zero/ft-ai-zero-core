# jobs/clean_and_rollup.py
import sqlite3, time, datetime as dt
from utils.config import SIMU_TRADES_DB, AI_PARAMS_DB

NOW = int(time.time())
FROM_7D = NOW - 7*24*3600

def q(c, sql, args=()):
    return c.execute(sql, args).fetchall()

def main():
    print("[DB] SIMU:", SIMU_TRADES_DB)
    print("[DB] AI  :", AI_PARAMS_DB)
    conn_s = sqlite3.connect(SIMU_TRADES_DB)
    conn_a = sqlite3.connect(AI_PARAMS_DB)

    # 1) 确保表结构
    conn_a.execute("""
    CREATE TABLE IF NOT EXISTS ai_snapshots(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        param_group_id INTEGER,
        window TEXT,
        trades INTEGER,
        win_rate REAL,
        score REAL,
        to_ts INTEGER,
        created_at TEXT DEFAULT (datetime('now'))
    )
    """)
    # 2) 清理垃圾行（param_group_id 为 NULL）
    conn_a.execute("DELETE FROM ai_snapshots WHERE param_group_id IS NULL")
    conn_a.commit()

    # 3) 从最近7天的 simu_trades 聚合（这里没有真实盈亏，就先用占位 win_rate=0.5；score=trades）
    rows = q(conn_s, """
        SELECT param_group_id, COUNT(*) AS n, MAX(ts) AS to_ts
        FROM trades
        WHERE ts>=? AND status='FILLED' AND param_group_id IS NOT NULL
        GROUP BY param_group_id
        HAVING n>0
        ORDER BY n DESC
    """, (FROM_7D,))

    if not rows:
        print("[WARN] 最近7天没有可聚合的成交（带 param_group_id 的）。")
    else:
        print(f"[ROLLUP] 将写入 {len(rows)} 条 snapshot ...")
        for gid, n, to_ts in rows:
            # 占位：win_rate 用 0.5；score 用成交数（后面有真实盈亏后再替换计算公式）
            conn_a.execute("""
                INSERT INTO ai_snapshots(param_group_id, window, trades, win_rate, score, to_ts)
                VALUES(?,?,?,?,?,?)
            """, (gid, "7d", int(n), 0.5, float(n), int(to_ts)))
        conn_a.commit()

    # 4) 打印快照前 10 条
    snaps = q(conn_a, "SELECT param_group_id,window,trades,win_rate,score,to_ts FROM ai_snapshots ORDER BY to_ts DESC LIMIT 10")
    print("最近快照：")
    for r in snaps:
        print(" ", r)

    conn_s.close(); conn_a.close()
    print("[DONE] clean_and_rollup finished.")

if __name__ == "__main__":
    main()
