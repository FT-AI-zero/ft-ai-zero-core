import os, sqlite3
from pathlib import Path
from utils.config import DATA_ROOT, DATA_NS, SHARED_ROOT

REQUIRED_TRADES = {
    "instId": "TEXT",
    "action": "TEXT",
    "price":  "REAL",
    "vol":    "REAL",
    "status": "TEXT",
    "comment":"TEXT",
    "ts":     "INTEGER",
}
PRICE_SRC   = ("fillPx","px","avgPx","execPx","matchPx")
VOL_SRC     = ("fillSz","sz","accFillSz","size","qty","amount")
COMMENT_SRC = ("remark","info","msg","note","commentText")
STATUS_SRC  = ("state","statusText")

def ensure_trades(conn, dbpath):
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='trades'")
    if not cur.fetchone(): return

    cols_now = {r[1]: r[2] for r in cur.execute("PRAGMA table_info(trades)")}
    # 补列
    for col, typ in REQUIRED_TRADES.items():
        if col not in cols_now:
            cur.execute(f"ALTER TABLE trades ADD COLUMN {col} {typ}")
            conn.commit()
            print(f"[add] {dbpath} -> trades.{col} {typ}")

    # 回填
    cols_now = {r[1] for r in cur.execute("PRAGMA table_info(trades)")}
    for c in PRICE_SRC:
        if c in cols_now:
            cur.execute(f"UPDATE trades SET price=COALESCE(price, CAST({c} AS REAL)) WHERE price IS NULL OR price=0")
            conn.commit()
            if cur.rowcount:
                print(f"[backfill] {dbpath} -> price from {c}, rows={cur.rowcount}")

    cols_now = {r[1] for r in cur.execute("PRAGMA table_info(trades)")}
    for c in VOL_SRC:
        if c in cols_now:
            cur.execute(f"UPDATE trades SET vol=COALESCE(vol, CAST({c} AS REAL)) WHERE vol IS NULL OR vol=0")
            conn.commit()
            if cur.rowcount:
                print(f"[backfill] {dbpath} -> vol from {c}, rows={cur.rowcount}")

    cols_now = {r[1] for r in cur.execute("PRAGMA table_info(trades)")}
    for c in COMMENT_SRC:
        if c in cols_now:
            cur.execute(f"UPDATE trades SET comment=COALESCE(NULLIF(comment,''), CAST({c} AS TEXT)) WHERE comment IS NULL OR comment=''")
            conn.commit()
            if cur.rowcount:
                print(f"[backfill] {dbpath} -> comment from {c}, rows={cur.rowcount}")

    for c in STATUS_SRC:
        if c in cols_now:
            cur.execute(f"UPDATE trades SET status=COALESCE(NULLIF(status,''), CAST({c} AS TEXT)) WHERE status IS NULL OR status=''")
            conn.commit()
            if cur.rowcount:
                print(f"[backfill] {dbpath} -> status from {c}, rows={cur.rowcount}")

    # 兜底
    cur.execute("UPDATE trades SET price   = COALESCE(price, 0)")
    cur.execute("UPDATE trades SET vol     = COALESCE(vol,   0)")
    cur.execute("UPDATE trades SET comment = COALESCE(comment, '')")
    cur.execute("UPDATE trades SET status  = COALESCE(status,  '')")
    cur.execute("UPDATE trades SET ts      = COALESCE(ts, 0)")
    conn.commit()

def ensure_signals(conn, dbpath):
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='signals'")
    if not cur.fetchone(): return
    cols = [r[1] for r in cur.execute("PRAGMA table_info(signals)")]
    if "meta" not in cols:
        cur.execute("ALTER TABLE signals ADD COLUMN meta TEXT")
        conn.commit()
        print(f"[add] {dbpath} -> signals.meta")

def scan_and_migrate(root: Path):
    touched = 0
    for base, _, files in os.walk(root):
        if os.path.basename(base) != "dbs": continue
        for fn in files:
            if not fn.lower().endswith(".db"): continue
            dbpath = os.path.join(base, fn)
            try:
                con = sqlite3.connect(dbpath)
                ensure_trades(con, dbpath)
                ensure_signals(con, dbpath)
                con.close()
                touched += 1
            except Exception as e:
                print(f"[ERR] {dbpath}: {e}")
    print(f"[done] migrated DBs: {touched}")

if __name__ == "__main__":
    # 命名空间根（paper/live）+ shared 都跑一遍
    ns_root = Path(DATA_ROOT) / DATA_NS
    scan_and_migrate(ns_root)
    scan_and_migrate(Path(DATA_ROOT) / "paper")
    scan_and_migrate(Path(DATA_ROOT) / "live")
    scan_and_migrate(SHARED_ROOT)
