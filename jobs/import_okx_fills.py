# jobs/import_okx_fills.py
import argparse, time, sqlite3, datetime
from utils.config import TRADES_DB
from core.okx_trader import OKXTrader

"""
把 OKX 私有 fills-history 导入统一 trades 表
 - mode='live'  source='api_import'  status='filled'
 - 避免重复：对 (instId, side, price, vol, ts) 做去重插入
"""

def ensure_trades_schema(db):
    con=sqlite3.connect(db); cur=con.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS trades(
        id INTEGER PRIMARY KEY,
        instId TEXT, action TEXT, price REAL, vol REAL,
        status TEXT, comment TEXT, ts TEXT
    )""")
    # 去重索引（可放宽）：同秒+同价+同量+方向 视为同一笔
    cur.execute("""CREATE UNIQUE INDEX IF NOT EXISTS ux_trades_key
                   ON trades(instId, action, price, vol, ts)""")
    con.commit(); con.close()

def iso_to_sec(s):
    # OKX 返回 "2025-08-10T02:11:22.123Z" 或 ms
    if not s: return int(time.time())
    try:
        return int(datetime.datetime.fromisoformat(s.replace("Z","+00:00")).timestamp())
    except:
        try:
            return int(int(s)/1000)
        except:
            return int(time.time())

def main(days):
    ensure_trades_schema(TRADES_DB)
    tr = OKXTrader()
    con=sqlite3.connect(TRADES_DB); cur=con.cursor()
    since = int(time.time()) - days*86400

    # OKXTrader.get_fills 已经封装过历史接口（fills-history）
    # 分页内部 OKX 会处理，这里简单拉取多次
    all_rows = []
    try:
        data = tr.get_fills(instType="SWAP", limit=100)
        # data 格式：{"code":"0","data":[{...},...]}
        arr = (data or {}).get("data", []) if isinstance(data, dict) else []
        all_rows.extend(arr)
    except Exception:
        pass

    ins, skip = 0, 0
    for r in all_rows:
        try:
            instId = r.get("instId")
            side   = "buy" if (r.get("side") == "buy") else ("sell" if r.get("side")=="sell" else (r.get("action") or ""))
            price  = float(r.get("fillPx") or r.get("px") or r.get("avgPx") or 0)
            vol    = float(r.get("fillSz") or r.get("sz") or 0)
            ts_sec = iso_to_sec(r.get("ts"))
            if ts_sec < since: 
                skip += 1; 
                continue
            ts_iso = datetime.datetime.utcfromtimestamp(ts_sec).strftime("%Y-%m-%d %H:%M:%S")
            comment = f"source=api_import; ordId={r.get('ordId','')}; gid={r.get('tag','')}"
            cur.execute("""INSERT OR IGNORE INTO trades(instId,action,price,vol,status,comment,ts)
                           VALUES(?,?,?,?,?,?,?)""",
                        (instId, "buy" if side=="buy" else "sell", price, vol, "filled", comment, ts_iso))
            if cur.rowcount: ins += 1
        except Exception:
            pass

    con.commit(); con.close()
    print(f"[fills_import] inserted={ins} skipped_old={skip} (days={days})")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=7)
    args = ap.parse_args()
    main(args.days)
