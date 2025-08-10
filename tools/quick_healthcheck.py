# -*- coding: utf-8 -*-
# tools/quick_healthcheck.py
import os, sys, sqlite3, json, datetime

# 让脚本无论从哪里运行都能找到工程根的 utils
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from utils.config import DB_DIR, LOG_DIR, SIGNAL_POOL_DB, STRATEGY_POOL_DB, TRADES_DB

REVIEW_DB = os.path.join(DB_DIR, "review.db")
ZERO_LOG  = os.path.join(LOG_DIR, "zero_engine.log")
LIVE_DIST_LOG = os.path.join(LOG_DIR, "live_dist.log")

def ts2iso(ts):
    try:
        ts = int(ts or 0)
        if ts <= 0: return "-"
        return datetime.datetime.utcfromtimestamp(ts).isoformat()
    except: return str(ts)

def tail(path, n=50):
    if not os.path.exists(path):
        return f"[{path}] 不存在"
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            lines = f.readlines()
            return "".join(lines[-n:])
    except Exception as e:
        return f"[读取失败] {path}: {e}"

def q(conn, sql, args=()):
    cur = conn.execute(sql, args)
    return cur.fetchall()

def print_dyn_table(conn, table, title=None, limit=10):
    """动态读取列名，兼容不同版本表结构"""
    try:
        cur = conn.execute(f"SELECT * FROM {table} ORDER BY ROWID DESC LIMIT ?", (limit,))
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description] if cur.description else []
    except sqlite3.OperationalError as e:
        print(f"\n== {title or table} ==")
        print(f"[WARN] 无法读取表 {table}: {e}")
        return
    print(f"\n== {title or table} ==")
    if not rows:
        print("(空)")
        return
    print(" | ".join(cols))
    for r in rows:
        out = []
        for v in r:
            out.append(str(v))
        print(" | ".join(out))

def main():
    print("【FT 快速体检】")
    print(f"DB_DIR={DB_DIR}")
    print(f"LOG_DIR={LOG_DIR}\n")

    # ---- signals 池 ----
    con = sqlite3.connect(SIGNAL_POOL_DB)
    try:
        cnts = q(con, "SELECT status, COUNT(*) FROM signals GROUP BY status ORDER BY 2 DESC")
        print("== signals 状态分布 ==")
        if cnts:
            for s, c in cnts:
                print(f"{s:>14}: {c}")
        else:
            print("(空)")

        rows = q(con, "SELECT id,instId,status,ts,meta FROM signals ORDER BY id DESC LIMIT 10")
        print("\n== signals 最近10条 ==")
        for rid, inst, st, ts, meta in rows:
            gid = None
            try:
                m = json.loads(meta) if meta else {}
                gid = m.get("param_group_id") or m.get("gid")
            except: pass
            print(f"{rid:>6} | {inst:<16} | {st:<12} | {ts2iso(ts)} | gid={gid}")
    finally:
        con.close()

    # ---- allowlist ----
    con = sqlite3.connect(STRATEGY_POOL_DB)
    try:
        rows = q(con, """
            SELECT param_group_id, window, score, trades, source, COALESCE(weight, score) AS w, created_at
            FROM allowlist
            ORDER BY COALESCE(weight, score) DESC, created_at DESC
            LIMIT 10
        """)
        print("\n== allowlist TOP10 ==")
        for gid, win, score, trades, src, w, ca in rows:
            print(f"gid={gid:<6} w={float(w or 0):>7.4f} | win={win:<6} | score={score:<8} | trades={trades:<4} | src={src:<12} | {ca}")
    finally:
        con.close()

    # ---- trades.db（动态，兼容无 id 版本）----
    con = sqlite3.connect(TRADES_DB)
    try:
        print_dyn_table(con, "trades", title="trades.db 最近10条", limit=10)
    finally:
        con.close()

    # ---- review.live_trades（也做动态以防历史差异）----
    if os.path.exists(REVIEW_DB):
        con = sqlite3.connect(REVIEW_DB)
        try:
            print_dyn_table(con, "live_trades", title="review.live_trades 最近10条", limit=10)
        finally:
            con.close()
    else:
        print("\n(review.db 不存在，略)")

    # ---- 日志尾巴 ----
    print("\n== zero_engine.log 尾巴(50行) ==")
    print(tail(ZERO_LOG, 50))
    print("== live_dist.log 尾巴(50行) ==")
    print(tail(LIVE_DIST_LOG, 50))

    print("\n【体检完成】")

if __name__ == "__main__":
    main()
