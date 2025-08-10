# jobs/create_learning_views.py
import sqlite3
from utils.config import TRADES_DB, REVIEW_DB

"""
学习统一视图说明：
- v_learning_trades：从 trades (paper/live/api_import) + review.live_trades 汇总
- 字段：instId, side, price, vol, ts, weight, source
- 权重：paper=0.5, live=1.0, api_import=1.0（可调）
- 规则：通过 comment/status 解析来源；review.live_trades 默认 live, weight=1.0
"""

def main():
    con = sqlite3.connect(TRADES_DB); cur=con.cursor()
    cur.executescript("""
    CREATE VIEW IF NOT EXISTS v_learning_trades AS
    SELECT 
        t.instId,
        CASE WHEN lower(t.action) in ('buy','long','open_long') THEN 'buy'
             WHEN lower(t.action) in ('sell','short','open_short') THEN 'sell'
             ELSE t.action END as side,
        t.price, t.vol, t.ts,
        CASE 
           WHEN instr(lower(coalesce(t.comment,'')), 'source=api_import')>0 THEN 1.0
           WHEN instr(lower(coalesce(t.comment,'')), 'mode=live')>0 THEN 1.0
           WHEN instr(lower(coalesce(t.comment,'')), 'mode=paper')>0 THEN 0.5
           ELSE 0.8
        END as weight,
        CASE 
           WHEN instr(lower(coalesce(t.comment,'')), 'source=api_import')>0 THEN 'api_import'
           WHEN instr(lower(coalesce(t.comment,'')), 'mode=live')>0 THEN 'live'
           WHEN instr(lower(coalesce(t.comment,'')), 'mode=paper')>0 THEN 'paper'
           ELSE 'unknown'
        END as source
    FROM trades t
    UNION ALL
    SELECT
        lt.instId,
        CASE WHEN lower(lt.side) in ('buy','long') THEN 'buy'
             WHEN lower(lt.side) in ('sell','short') THEN 'sell'
             ELSE lt.side END as side,
        lt.price, lt.vol, lt.ts,
        1.0 as weight,
        'live_review' as source
    FROM main.live_trades lt -- 若 review.db 独立，可 ATTACH，再建视图（见下）
    ;
    """)
    con.commit(); con.close()
    print("[views] v_learning_trades created (note: if review.db is separate file, run the ATTACH version below)")

if __name__ == "__main__":
    main()
