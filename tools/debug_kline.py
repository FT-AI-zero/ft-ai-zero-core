# -*- coding: utf-8 -*-
import os, sys, sqlite3, datetime, time
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path: sys.path.insert(0, ROOT)

from utils.config import DB_DIR
from core.okx_trader import OKXTrader

REVIEW_DB = os.path.join(DB_DIR, "review.db")

def pick_one_trade():
    conn = sqlite3.connect(REVIEW_DB)
    row = conn.execute("SELECT id, instId, ts FROM live_trades ORDER BY id DESC LIMIT 1").fetchone()
    conn.close()
    return row

def iso_to_utc_epoch(s):
    # 兼容 '2025-08-09T09:43:44.928892'
    dt = datetime.datetime.fromisoformat(s.replace("Z","+00:00"))
    # 项目里 ts 是 UTC，无 tz 则按 UTC 处理
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    else:
        dt = dt.astimezone(datetime.timezone.utc)
    return int(dt.timestamp())

def iso(ts):  # 秒 -> UTC 可读
    return datetime.datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")

if __name__ == "__main__":
    t = OKXTrader()
    row = pick_one_trade()
    if not row:
        print("[debug] no live_trades rows"); sys.exit(0)

    _id, instId, ts = row
    trade_sec = iso_to_utc_epoch(ts)
    start = trade_sec - 3600   # 前 1 小时
    end   = trade_sec + 7200   # 后 2 小时
    print(f"[debug] trade_id={_id} {instId} trade_ts={iso(trade_sec)} window=[{iso(start)} ~ {iso(end)}]")

    rows = t.get_kline_range(instId, bar="1m", start_ts=start, end_ts=end, limit_per_page=300, max_pages=120)
    print(f"[debug] got {len(rows)} rows")
    if rows:
        print("[debug] first =", iso(rows[0][0]), " last =", iso(rows[-1][0]))
        print("[debug] sample(3) =", rows[:3])
    else:
        # 额外给点上下文，方便判断是不是时间窗问题
        latest = t.get_kline_range(instId, bar="1m", start_ts=int(time.time())-3600, end_ts=int(time.time()))
        if latest:
            print(f"[debug] latest window 1h got {len(latest)} rows; first={iso(latest[0][0])} last={iso(latest[-1][0])}")
        else:
            print("[debug] latest 1h also 0 rows -> 检查网络/账号/合约ID")
