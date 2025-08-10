# -*- coding: utf-8 -*-
# tools/insert_signal.py
import os, sys, time, json, sqlite3, argparse, datetime

# 让脚本无论从哪里运行都能找到工程根的 utils
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from utils.config import SIGNAL_POOL_DB

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--instId", required=True)
    ap.add_argument("--period", default="1m")
    ap.add_argument("--price", type=float, required=True)
    ap.add_argument("--vol", type=float, default=0.01)
    ap.add_argument("--side", choices=["buy", "sell"], required=True)
    ap.add_argument("--gid", type=int, required=True)
    ap.add_argument("--status", choices=["WAIT_SIMU", "WAIT_LIVE"], default="WAIT_SIMU")
    ap.add_argument("--lev", type=int, default=50)  # 默认高一点，避免 SIZE_ZERO
    ap.add_argument("--signal_type", default="manual")
    ap.add_argument("--priority", type=int, default=3)
    ap.add_argument("--ttl", type=int, default=0, help="过期秒数；0 表示不过期")
    args = ap.parse_args()

    ts = int(time.time())
    expire_ts = ts + args.ttl if args.ttl and args.ttl > 0 else 0

    meta = {
        "param_group_id": args.gid,
        "side": args.side,
        "lev": args.lev
    }

    conn = sqlite3.connect(SIGNAL_POOL_DB)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO signals(instId, interval, period, ts, close, vol, signal_type, status, detected_at, meta, priority, promotion_level, expire_ts)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        args.instId, args.period, args.period, ts, args.price, args.vol,
        args.signal_type, args.status, datetime.datetime.utcnow().isoformat(),
        json.dumps(meta, ensure_ascii=False), args.priority, 0, expire_ts
    ))
    conn.commit()
    sid = cur.lastrowid
    conn.close()
    print(f"[ok] inserted signal id={sid}, status={args.status}, gid={args.gid}, instId={args.instId}, price={args.price}, lev={args.lev}")

if __name__ == "__main__":
    main()
