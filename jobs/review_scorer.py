# jobs/review_scorer.py
import sqlite3
from pathlib import Path
from utils.config import REVIEW_DB

REVIEW_SCORES_SQL = """
CREATE TABLE IF NOT EXISTS review_scores (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT,
    instId TEXT,
    trades INTEGER,
    pnl REAL,
    volume REAL,
    win_rate REAL,
    avg_pnl REAL,
    score REAL
);
"""

REVIEW_DAILY_SQL = """
CREATE TABLE IF NOT EXISTS review_daily (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT,
    trades INTEGER,
    pnl REAL,
    volume REAL,
    win_rate REAL,
    avg_pnl REAL,
    score REAL
);
"""

def run():
    conn = sqlite3.connect(REVIEW_DB)
    try:
        c = conn.cursor()
        c.execute(REVIEW_SCORES_SQL)
        c.execute(REVIEW_DAILY_SQL)

        # 逐品种逐日
        c.execute("""
            SELECT 
                substr(ts, 1, 10) AS d, instId,
                COUNT(*) AS n,
                COALESCE(SUM(pnl), 0.0) AS pnl_sum,
                COALESCE(SUM(vol), 0.0) AS vol_sum,
                AVG(COALESCE(pnl,0)) AS avg_pnl,
                SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS win_rate
            FROM review
            WHERE ts NOT NULL AND instId NOT NULL
            GROUP BY d, instId
            ORDER BY d DESC
        """)
        rows = c.fetchall()

        # 清空当日重算（简单处理，也可做 UPSERT）
        # 这里直接插入，不去重；如需去重可先DELETE指定日期（略）
        inserted = 0
        for d, inst, n, pnl_sum, vol_sum, avg_pnl, win_rate in rows:
            # 简单评分：盈利+胜率+成交量权重（可换成AI评分）
            score = (pnl_sum * 0.6) + (win_rate or 0) * 100 * 0.3 + (vol_sum or 0) * 0.1
            c.execute("""
                INSERT INTO review_scores (date, instId, trades, pnl, volume, win_rate, avg_pnl, score)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (d, inst, n, pnl_sum, vol_sum, win_rate or 0.0, avg_pnl or 0.0, score))
            inserted += 1

        # 汇总逐日
        c.execute("""
            SELECT 
                substr(ts, 1, 10) AS d,
                COUNT(*) AS n,
                COALESCE(SUM(pnl), 0.0) AS pnl_sum,
                COALESCE(SUM(vol), 0.0) AS vol_sum,
                AVG(COALESCE(pnl,0)) AS avg_pnl,
                SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS win_rate
            FROM review
            WHERE ts NOT NULL
            GROUP BY d
            ORDER BY d DESC
        """)
        rows2 = c.fetchall()

        for d, n, pnl_sum, vol_sum, avg_pnl, win_rate in rows2:
            score = (pnl_sum * 0.6) + (win_rate or 0) * 100 * 0.3 + (vol_sum or 0) * 0.1
            c.execute("""
                INSERT INTO review_daily (date, trades, pnl, volume, win_rate, avg_pnl, score)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (d, n, pnl_sum, vol_sum, win_rate or 0.0, avg_pnl or 0.0, score))

        conn.commit()
        print(f"[review_scorer] inserted rows: score={inserted}, daily={len(rows2)}")
    finally:
        conn.close()

if __name__ == "__main__":
    run()
