# jobs/push_test_live_signal.py
import sqlite3, time, json, argparse, datetime as dt
from utils.config import SIGNAL_POOL_DB

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--instId", required=True)
    ap.add_argument("--side",   required=True, choices=["buy","sell"])
    ap.add_argument("--gid",    type=int, required=True)
    ap.add_argument("--price",  type=float, required=True)
    ap.add_argument("--vol",    type=float, default=1.0)
    ap.add_argument("--tp",     type=float)
    ap.add_argument("--sl",     type=float)
    ap.add_argument("--period", default="1m")
    ap.add_argument("--expire", type=int, default=0, help="过期时间戳(秒)，0表示不过期")
    args = ap.parse_args()

    meta = {"param_group_id": args.gid, "side": args.side}
    if args.tp is not None: meta["tp"] = args.tp
    if args.sl is not None: meta["sl"] = args.sl

    now = int(time.time())
    conn = sqlite3.connect(SIGNAL_POOL_DB)
    conn.execute("""
    CREATE TABLE IF NOT EXISTS signals(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        instId TEXT, interval TEXT, period TEXT, ts INTEGER, close REAL, vol REAL,
        signal_type TEXT, status TEXT, detected_at TEXT, meta TEXT,
        priority INTEGER, promotion_level INTEGER, expire_ts INTEGER
    )""")
    conn.execute("""
    INSERT INTO signals(instId, period, ts, close, vol, signal_type, status, detected_at, meta, priority, promotion_level, expire_ts)
    VALUES(?,?,?,?,?, ?,?,?,?, ?,?,?)
    """, (
        args.instId, args.period, now, args.price, args.vol,
        "BREAKOUT_UP" if args.side=="buy" else "BREAKOUT_DOWN",
        "WAIT_LIVE", dt.datetime.utcnow().isoformat(), json.dumps(meta, ensure_ascii=False),
        3, 0, args.expire
    ))
    conn.commit(); conn.close()
    print(f"[PUSHED] {args.instId} {args.side} gid={args.gid} price={args.price} vol={args.vol}")

if __name__ == "__main__":
    main()
