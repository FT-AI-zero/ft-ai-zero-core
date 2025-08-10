# -*- coding: utf-8 -*-
# jobs/pnl_replay.py

import os, sys, time, json, math, sqlite3, datetime, argparse
from typing import List, Tuple

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from utils.config import DB_DIR
from core.okx_trader import OKXTrader

REVIEW_DB = os.path.join(DB_DIR, "review.db")

# ---- 可调参数（支持环境变量）----
BAR              = os.getenv("PNL_BAR", "1m")
TP_PCT           = float(os.getenv("PNL_TP_PCT", "0.006"))   # 0.6%
SL_PCT           = float(os.getenv("PNL_SL_PCT", "0.004"))   # 0.4%
MAX_HOLD_MIN     = int(os.getenv("PNL_MAX_HOLD_MIN", "240")) # 最多持有 240 分钟
TAKER_FEE_RATE   = float(os.getenv("PNL_TAKER_FEE", "0.0005"))  # 5bps；按需改
TRAIL_RATIO      = float(os.getenv("PNL_TRAIL_RATIO", "0.0"))   # 0 表示不启用追踪

def open_db(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn

def ensure_schema():
    conn = open_db(REVIEW_DB)
    cur  = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS live_trades(
        id INTEGER PRIMARY KEY,
        instId TEXT, side TEXT, price REAL, vol REAL, gid INTEGER, ts TEXT
    )""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS pnl_by_trade(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        live_trade_id INTEGER UNIQUE,
        instId TEXT, gid INTEGER, side TEXT,
        open_ts TEXT, close_ts TEXT,
        entry REAL, exit REAL, qty REAL,
        hold_sec INTEGER,
        tp REAL, sl REAL, trail_ratio REAL,
        exit_reason TEXT,
        taker_fee_rate REAL, fees REAL,
        pnl REAL, pnl_pct REAL,
        mfe REAL, mae REAL
    )""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS kline_cache(
        instId TEXT, bar TEXT, ts INTEGER,
        open REAL, high REAL, low REAL, close REAL, vol REAL,
        PRIMARY KEY(instId,bar,ts)
    )""")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_kline_cache ON kline_cache(instId,bar,ts)")
    conn.commit(); conn.close()

def iso2dt(s:str) -> datetime.datetime:
    try:
        if "Z" in s or "+" in s:
            dt = datetime.datetime.fromisoformat(s.replace("Z", "+00:00"))
            return dt.astimezone(datetime.timezone.utc).replace(tzinfo=None)
        return datetime.datetime.fromisoformat(s)
    except Exception:
        try:
            return datetime.datetime.utcfromtimestamp(int(s))
        except:
            return datetime.datetime.utcnow()

def dt2epoch_sec_utc(dt: datetime.datetime) -> int:
    if dt.tzinfo is None:
        return int(dt.replace(tzinfo=datetime.timezone.utc).timestamp())
    return int(dt.timestamp())

def fetch_recent_live_trades(limit=200) -> List[Tuple]:
    conn = open_db(REVIEW_DB)
    cur = conn.cursor()
    rows = cur.execute("""
      SELECT t.id, t.instId, t.side, t.price, t.vol, t.gid, t.ts
      FROM live_trades t
      LEFT JOIN pnl_by_trade p ON p.live_trade_id = t.id
      WHERE p.id IS NULL
      ORDER BY t.id ASC
      LIMIT ?
    """, (limit,)).fetchall()
    conn.close()
    return rows

def kline_from_okx(t:OKXTrader, instId:str, start_ts:int, mins:int) -> List[Tuple[int,float,float,float,float,float]]:
    """
    拉 [start, start+mins] 的1mK线，优先从缓存命中，不足再从 OKX 翻页抓取。
    统一返回 [(ts, o,h,l,c,vol)] 升序。
    """
    end_ts = start_ts + mins*60 + 60
    conn = open_db(REVIEW_DB)
    cur = conn.cursor()

    # 先从缓存拿
    cached = cur.execute("""
        SELECT ts,open,high,low,close,vol
          FROM kline_cache
         WHERE instId=? AND bar=? AND ts BETWEEN ? AND ?
         ORDER BY ts ASC
    """, (instId, BAR, start_ts, end_ts)).fetchall()
    have = {r[0] for r in cached}

    # 不足再补
    need_missing = (end_ts - start_ts + 1) // 60 + 1
    if len(cached) < need_missing:
        rows = t.get_kline_range(instId, bar=BAR,
                                 start_ts=start_ts-60, end_ts=end_ts+60,
                                 limit_per_page=200, max_pages=60) or []
        # rows: [(ts,o,h,l,c,v)] 升序
        ins = []
        for ts,o,h,l,c,v in rows:
            if ts<start_ts-60 or ts> end_ts+60: continue
            if ts in have: continue
            ins.append((instId,BAR,ts,o,h,l,c,v))
            have.add(ts)
        if ins:
            cur.executemany("""
                INSERT OR IGNORE INTO kline_cache(instId,bar,ts,open,high,low,close,vol)
                VALUES(?,?,?,?,?,?,?,?)
            """, ins)
            conn.commit()

    out = cur.execute("""
        SELECT ts,open,high,low,close,vol
          FROM kline_cache
         WHERE instId=? AND bar=? AND ts BETWEEN ? AND ?
         ORDER BY ts ASC
    """, (instId, BAR, start_ts, end_ts)).fetchall()
    conn.close()
    return out

def triple_barrier(side:str, entry:float, kls:List[Tuple]) -> Tuple[float,str,float,float,int]:
    """
    返回 (exit_price, reason, mfe, mae, hold_sec)
    """
    if not kls: return (entry, "no_kline", 0.0, 0.0, 0)
    up  = entry*(1+TP_PCT)
    dn  = entry*(1-SL_PCT)
    best = entry
    worst= entry
    trail_anchor = None

    start_ts = kls[0][0]
    end_ts   = kls[-1][0]

    for ts,o,h,l,c,v in kls:
        # 以 high/low 估 MFE/MAE；空头做镜像
        if side == "buy":
            best = max(best, h); worst = min(worst, l)
        else:
            # 映射：对空头把价格越低视为“更高的收益”
            best = max(best, 2*entry - l)
            worst= min(worst, 2*entry - h)

        # 追踪止盈（可选）
        if TRAIL_RATIO>0:
            if side=="buy":
                trail_anchor = max(trail_anchor or c, h)
                dn = max(dn, trail_anchor*(1-TRAIL_RATIO))
            else:
                trail_anchor = min(trail_anchor or c, l)
                up = min(up, trail_anchor*(1+TRAIL_RATIO))

        # 触发顺序：先触底/后触顶（近似处理）
        if side=="buy":
            if l <= dn: return (dn, "sl", best-entry, entry-worst, ts-start_ts)
            if h >= up: return (up, "tp", best-entry, entry-worst, ts-start_ts)
        else:
            if h >= up: return (up, "sl", best-entry, entry-worst, ts-start_ts)
            if l <= dn: return (dn, "tp", best-entry, entry-worst, ts-start_ts)

    # 超时：用最后收盘
    exit_px = kls[-1][3]
    return (exit_px, "timeout", best-entry, entry-worst, end_ts-start_ts)

def fees(entry, exit_px, qty):
    gross = abs(entry*qty) + abs(exit_px*qty)
    return gross * TAKER_FEE_RATE

def process_batch(limit=200):
    t = OKXTrader()
    rows = fetch_recent_live_trades(limit=limit)
    if not rows:
        print("[pnl] no new rows"); return 0

    conn = open_db(REVIEW_DB)
    cur  = conn.cursor()
    done = 0
    for _id, instId, side, price, vol, gid, ts in rows:
        try:
            open_dt   = iso2dt(ts)
            start_sec = dt2epoch_sec_utc(open_dt)
            kls = kline_from_okx(t, instId, start_sec, MAX_HOLD_MIN)
            exit_px, reason, mfe_val, mae_val, hold_sec = triple_barrier(side, float(price), kls)

            q = float(vol or 0)
            f = fees(price, exit_px, q)
            pnl_abs = (exit_px - price)*q if side=="buy" else (price - exit_px)*q
            pnl_pct = (exit_px/price - 1.0) if side=="buy" else (price/exit_px - 1.0)

            cur.execute("""
                INSERT OR IGNORE INTO pnl_by_trade(
                    live_trade_id, instId, gid, side, open_ts, close_ts,
                    entry, exit, qty, hold_sec, tp, sl, trail_ratio, exit_reason,
                    taker_fee_rate, fees, pnl, pnl_pct, mfe, mae
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                _id, instId, gid, side, ts,
                (open_dt + datetime.timedelta(seconds=hold_sec)).isoformat(),
                price, exit_px, q, hold_sec,
                price*(1+TP_PCT), price*(1-SL_PCT), TRAIL_RATIO, reason,
                TAKER_FEE_RATE, f, pnl_abs - f, pnl_pct, mfe_val, mae_val
            ))
            done += 1
        except Exception as e:
            print(f"[pnl] row {_id} err={e}")
    conn.commit(); conn.close()
    print(f"[pnl] done={done}")
    return done

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--once", action="store_true", help="只跑一轮")
    p.add_argument("--sleep", type=float, default=3.0, help="空转时休眠秒数")
    p.add_argument("--limit", type=int, default=200, help="每轮回放数量上限")
    return p.parse_args()

def main():
    ensure_schema()
    args = parse_args()
    while True:
        n = process_batch(limit=args.limit)
        if args.once:
            break
        time.sleep(args.sleep if n == 0 else 0.5)

if __name__ == "__main__":
    main()
