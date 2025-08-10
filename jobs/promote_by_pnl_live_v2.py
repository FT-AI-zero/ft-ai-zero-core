# jobs/promote_by_pnl_live_v2.py  —— 替换版
import os, sqlite3, datetime as dt
from utils.config import DB_DIR, AI_PARAMS_DB

REVIEW_DB = os.path.join(DB_DIR, "review.db")

SCHEMASQL_CANDIDATES = """
CREATE TABLE IF NOT EXISTS candidates(
  param_group_id INTEGER,
  window TEXT,
  trades INTEGER,
  score REAL,
  source TEXT,
  created_at TEXT
);
"""

def utcnow():
    return dt.datetime.utcnow().replace(microsecond=0)

def iso(ts: dt.datetime) -> str:
    return ts.isoformat()

def ensure_candidates_schema(ai_db: str):
    conn = sqlite3.connect(ai_db)
    cur = conn.cursor()
    cur.execute(SCHEMASQL_CANDIDATES)
    conn.commit()
    conn.close()

def aggregate_window(conn, window_days: int):
    """按 gid 聚合：近 window_days 的 sum(pnl)、count(trades)"""
    cur = conn.cursor()
    cutoff = iso(utcnow() - dt.timedelta(days=window_days))
    # 跳过 no_kline；open_ts 为 ISO 字符串，直接做字符串比较即可（ISO 可比较大小）
    cur.execute("""
      SELECT gid, COUNT(*) AS trades, COALESCE(SUM(pnl),0.0) AS score
      FROM pnl_by_trade
      WHERE exit_reason <> 'no_kline' AND open_ts >= ?
      GROUP BY gid
      HAVING trades > 0
      ORDER BY score DESC
    """, (cutoff,))
    rows = cur.fetchall()
    return rows

def main():
    try:
        ensure_candidates_schema(AI_PARAMS_DB)

        rconn = sqlite3.connect(REVIEW_DB)
        now_iso = iso(utcnow())

        agg = []
        agg_7d  = aggregate_window(rconn, 7)
        agg_30d = aggregate_window(rconn, 30)
        rconn.close()

        # 写入 ai_params.candidates
        aconn = sqlite3.connect(AI_PARAMS_DB)
        acur  = aconn.cursor()
        for gid, trades, score in agg_7d:
            acur.execute(
                "INSERT INTO candidates(param_group_id,window,trades,score,source,created_at) VALUES (?,?,?,?,?,?)",
                (gid, "7d", trades, float(score), "pnl_live_true", now_iso)
            )
        for gid, trades, score in agg_30d:
            acur.execute(
                "INSERT INTO candidates(param_group_id,window,trades,score,source,created_at) VALUES (?,?,?,?,?,?)",
                (gid, "30d", trades, float(score), "pnl_live_true", now_iso)
            )
        aconn.commit(); aconn.close()
        print("[promote_true] done")
    except Exception as e:
        print(f"[promote_true] error: {e}")

if __name__ == "__main__":
    main()
