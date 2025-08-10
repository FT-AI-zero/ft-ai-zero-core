# jobs/promote_by_pnl_live.py
import sqlite3, datetime as dt
from utils.config import REVIEW_DB, AI_PARAMS_DB
from core.okx_trader import OKXTrader

def q(c, sql, args=()):
    return c.execute(sql, args).fetchall()

def ensure_candidates(conn):
    conn.execute("""
    CREATE TABLE IF NOT EXISTS candidates(
        param_group_id INTEGER,
        window TEXT,
        trades INTEGER,
        win_rate REAL,
        score REAL,
        source TEXT,
        created_at TEXT,
        PRIMARY KEY(param_group_id, window, created_at)
    )""")
    conn.commit()

def calc_directional_pnl(entry_price: float, last: float, side: str):
    """返回方向性收益（仅用于排名，不做真实PnL）：涨对多/跌对空为正。"""
    if not entry_price or not last:
        return 0.0
    s = 1 if str(side).lower() == "buy" else -1
    return (last - entry_price) * s / entry_price  # 相对收益

def main():
    print("[DB] REVIEW:", REVIEW_DB)
    print("[DB] AI    :", AI_PARAMS_DB)
    conn_r = sqlite3.connect(REVIEW_DB)
    conn_a = sqlite3.connect(AI_PARAMS_DB)
    ensure_candidates(conn_a)
    today = dt.date.today().isoformat()

    # 优先用 pnl_by_trade（如果你以后建了真实PnL表会走这里）
    try:
        rows = q(conn_r, """
            SELECT gid as param_group_id,
                   COUNT(1) as trades,
                   AVG(CASE WHEN pnl>0 THEN 1.0 ELSE 0.0 END) as win_rate,
                   SUM(pnl) as pnl_sum
            FROM pnl_by_trade
            WHERE date(ts) >= date('now','-30 day') AND gid IS NOT NULL
            GROUP BY gid
            HAVING trades >= 3
            ORDER BY pnl_sum DESC
            LIMIT 50
        """)
        use_live = False
    except sqlite3.OperationalError:
        # 没有pnl_by_trade：降级用 live_trades + 现价方向收益
        use_live = True

    inserted = 0
    if not use_live:
        for gid, trades, win_rate, pnl_sum in rows:
            score = float(win_rate or 0) * 10.0  # 真实PnL可用更激进权重
            try:
                conn_a.execute("""
                  INSERT INTO candidates(param_group_id, window, trades, win_rate, score, source, created_at)
                  VALUES(?,?,?,?,?,'pnl_live',?)
                """, (int(gid), '30d', int(trades or 0), float(win_rate or 0), float(score), today))
                inserted += 1
            except sqlite3.IntegrityError:
                conn_a.execute("""
                  UPDATE candidates
                  SET trades=?, win_rate=?, score=?, source='pnl_live'
                  WHERE param_group_id=? AND window='30d' AND created_at=?
                """, (int(trades or 0), float(win_rate or 0), float(score), int(gid), today))
    else:
        # === 降级路径：用 live_trades ===
        t = OKXTrader()
        # 近30天按分组取最近的入场记录（同gid可能多instId）
        rows = q(conn_r, """
            SELECT gid, instId, side, price
            FROM live_trades
            WHERE date(ts) >= date('now','-30 day') AND gid IS NOT NULL
        """)
        # 聚合：gid -> [directional_pnl ...]
        from collections import defaultdict
        acc = defaultdict(list)
        for gid, inst, side, entry in rows:
            if not gid or not inst or not entry: 
                continue
            try:
                tk = t.get_ticker(inst) or {}
                last = float(tk.get("last") or tk.get("lastPx") or 0)
            except Exception:
                last = 0.0
            dp = calc_directional_pnl(float(entry or 0), last, side or "buy")
            acc[int(gid)].append(dp)

        for gid, vec in acc.items():
            trades = len(vec)
            if trades < 3: 
                continue
            wins = sum(1 for x in vec if x > 0)
            win_rate = wins / trades
            # 分数：胜率 *10 + 平均方向收益 *100（适中放大）
            score = win_rate * 10.0 + (sum(vec)/trades) * 100.0
            try:
                conn_a.execute("""
                  INSERT INTO candidates(param_group_id, window, trades, win_rate, score, source, created_at)
                  VALUES(?,?,?,?,?,'pnl_live_proxy',?)
                """, (gid, '30d', trades, win_rate, score, today))
                inserted += 1
            except sqlite3.IntegrityError:
                conn_a.execute("""
                  UPDATE candidates
                  SET trades=?, win_rate=?, score=?, source='pnl_live_proxy'
                  WHERE param_group_id=? AND window='30d' AND created_at=?
                """, (trades, win_rate, score, gid, today))

    conn_a.commit()
    print(f"[PROMOTE_PNL] upserted {inserted} candidates by pnl_live{' (proxy)' if use_live else ''}.")
    conn_r.close(); conn_a.close()

if __name__ == "__main__":
    main()
