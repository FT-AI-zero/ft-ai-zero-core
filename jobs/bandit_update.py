# jobs/bandit_update.py
import sqlite3
from utils.config import REVIEW_DB, STRATEGY_POOL_DB
from core.okx_trader import OKXTrader

def q(c, sql, args=()): 
    return c.execute(sql, args).fetchall()

def ensure_allowlist_cols(conn):
    cols = {r[1] for r in conn.execute("PRAGMA table_info(allowlist)").fetchall()}
    if "weight" not in cols:
        conn.execute("ALTER TABLE allowlist ADD COLUMN weight REAL")
    if "reason" not in cols:
        conn.execute("ALTER TABLE allowlist ADD COLUMN reason TEXT")
    conn.commit()

def main():
    print("[DB] REVIEW:", REVIEW_DB)
    print("[DB] SP    :", STRATEGY_POOL_DB)
    conn_r = sqlite3.connect(REVIEW_DB)
    conn_s = sqlite3.connect(STRATEGY_POOL_DB)
    ensure_allowlist_cols(conn_s)

    use_live = False
    try:
        rows = q(conn_r, """
          SELECT gid, COUNT(1) as n, AVG(pnl) as pnl_avg
          FROM pnl_by_trade
          WHERE date(ts) >= date('now','-7 day') AND gid IS NOT NULL
          GROUP BY gid HAVING n>=2
        """)
        # 真实PnL映射到 [0.5, 1.5]
        updated = 0
        for gid, n, pnl_avg in rows:
            weight = 1.0 + float(pnl_avg or 0)
            weight = max(0.5, min(1.5, weight))
            conn_s.execute("UPDATE allowlist SET weight=?, reason=? WHERE param_group_id=?",
                           (weight, "bandit_pnl_7d", int(gid)))
            updated += 1
        conn_s.commit()
        print(f"[BANDIT] updated {updated} weights (real pnl).")
    except sqlite3.OperationalError:
        # 降级：live_trades + 方向收益
        use_live = True
        t = OKXTrader()
        rows = q(conn_r, """
          SELECT gid, instId, side, price
          FROM live_trades
          WHERE date(ts) >= date('now','-7 day') AND gid IS NOT NULL
        """)
        from collections import defaultdict
        acc = defaultdict(list)
        for gid, inst, side, price in rows:
            if not gid or not inst or not price: 
                continue
            try:
                tk = t.get_ticker(inst) or {}
                last = float(tk.get("last") or tk.get("lastPx") or 0)
            except Exception:
                last = 0.0
            s = 1 if str(side).lower() == "buy" else -1
            r = (last - float(price)) * s / float(price) if last and price else 0.0
            acc[int(gid)].append(r)

        updated = 0
        for gid, vec in acc.items():
            if len(vec) < 2: 
                continue
            avg = sum(vec)/len(vec)
            weight = 1.0 + avg   # 简单映射
            weight = max(0.5, min(1.5, weight))
            conn_s.execute("UPDATE allowlist SET weight=?, reason=? WHERE param_group_id=?",
                           (weight, "bandit_dir_7d", gid))
            updated += 1
        conn_s.commit()
        print(f"[BANDIT] updated {updated} weights (direction proxy).")

    conn_r.close(); conn_s.close()

if __name__ == "__main__":
    main()
