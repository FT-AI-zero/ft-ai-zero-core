# jobs/health_check.py  —— 顶部替换
import sqlite3, datetime, time, os
from pathlib import Path
from utils import config as cfg

# 统一解析当前命名空间与目录
DATA_NS = getattr(cfg, "DATA_NS", os.environ.get("FT_DATA_NS", "paper"))
DB_DIR  = Path(getattr(cfg, "DB_DIR", Path(cfg.DATA_DIR) / "dbs"))

# 兜底拼接（避免常量缺失时报错）
def _db(name, default):
    return Path(getattr(cfg, name, default))

KLINE_1M_DB  = _db("KLINE_1M_DB",  DB_DIR / "kline_1m.db")
KLINE_3M_DB  = _db("KLINE_3M_DB",  DB_DIR / "kline_3m.db")
KLINE_5M_DB  = _db("KLINE_5M_DB",  DB_DIR / "kline_5m.db")
KLINE_15M_DB = _db("KLINE_15M_DB", DB_DIR / "kline_15m.db")
KLINE_1H_DB  = _db("KLINE_1H_DB",  DB_DIR / "kline_1H.db")
KLINE_4H_DB  = _db("KLINE_4H_DB",  DB_DIR / "kline_4H.db")
KLINE_1D_DB  = _db("KLINE_1D_DB",  DB_DIR / "kline_1D.db")

TRADES_DB    = _db("TRADES_DB",    DB_DIR / "trades.db")
REVIEW_DB    = _db("REVIEW_DB",    DB_DIR / "review.db")
SIGNAL_POOL_DB = _db("SIGNAL_POOL_DB", DB_DIR / "signals.db")
FEATURES_DB  = _db("FEATURES_DB",  DB_DIR / "features.db")

# 如果 health_check 里还有别的 DB 就照这个模式加一行


REQUIRED_TRADE_COLS = ("instId","action","price","vol","status","comment","ts")  # 我们已统一

def _fmt_age(ts):
    if ts is None: return "n/a"
    age = int(time.time()) - int(ts)
    return f"{age}s"

def _mtime(p):
    try: return int(Path(p).stat().st_mtime)
    except: return None

def check_db_exists_and_tables():
    print("=== DB & Tables ===")
    for file, tabs in REQUIRED_TABLES.items():
        # 支持 shared 里的 ai_params.db
        if file == os.path.basename(AI_PARAMS_DB):
            dbp = AI_PARAMS_DB
        else:
            dbp = os.path.join(DB_DIR, file)
        ok = os.path.exists(dbp)
        print(f"{file:18} exists={ok}  fresh={_fmt_age(_mtime(dbp))}  -> {dbp}")
        if not ok: 
            print("  !! missing file"); 
            continue
        con = sqlite3.connect(dbp); cur = con.cursor()
        for t in tabs:
            row = cur.execute("select count(1) from sqlite_master where type='table' and name=?", (t,)).fetchone()
            print(f"   - table {t:14} ->", "OK" if (row and row[0]) else "MISSING")
        # trade表列检查
        if file == "trades.db":
            try:
                cols = [r[1] for r in cur.execute("PRAGMA table_info(trades)")]
                miss = [c for c in REQUIRED_TRADE_COLS if c not in cols]
                print("   - trades cols:", cols)
                if miss: print("   !! missing cols:", miss)
                n_null = cur.execute("""select count(1) from trades 
                    where price is null or vol is null or status is null or comment is null""").fetchone()[0]
                print("   - trades NULL rows:", n_null)
            except Exception as e:
                print("   !! trades check error:", e)
        con.close()
    print()

def check_recency_sample_rows():
    print("=== Recency & Sample ===")
    items = [
        ("kline_1m.db","kline","select max(ts) from kline"),
        ("signals.db","signals","select max(ts) from signals"),
        ("trades.db","trades","select max(ts) from trades"),
        ("review.db","live_trades","select max(ts) from live_trades"),
        (os.path.basename(AI_PARAMS_DB),"ai_params","select max(updated_at) from ai_params"),
    ]
    for file, table, sql in items:
        dbp = AI_PARAMS_DB if file == os.path.basename(AI_PARAMS_DB) else os.path.join(DB_DIR, file)
        if not os.path.exists(dbp):
            print(f"{file:18} -> missing"); continue
        con = sqlite3.connect(dbp); cur = con.cursor()
        try:
            ts = cur.execute(sql).fetchone()[0]
            print(f"{file:18} {table:14} last_ts={ts} age={_fmt_age(ts)}")
            # 抽样
            try:
                row = cur.execute(f"select * from {table} order by rowid desc limit 1").fetchone()
                print("   sample:", row)
            except Exception:
                pass
        except Exception as e:
            print(f"{file:18} {table:14} check error:", e)
        con.close()
    print()

def check_logs_tail():
    print("=== Logs tail (last 3 lines each) ===")
    if not os.path.isdir(LOG_DIR):
        print("no log dir:", LOG_DIR); return
    for fn in sorted(os.listdir(LOG_DIR)):
        p = os.path.join(LOG_DIR, fn)
        if not os.path.isfile(p): continue
        try:
            with open(p, "r", encoding="utf-8", errors="ignore") as f:
                lines = f.readlines()[-3:]
            print(f"[{fn}]")
            for L in lines:
                print("   ", L.rstrip())
        except Exception as e:
            print(f"[{fn}] read error:", e)
    print()

def main():
    print(f"[health_check] MODE={MODE} DATA_DIR={DATA_DIR}")
    check_db_exists_and_tables()
    check_recency_sample_rows()
    check_logs_tail()
    print("[health_check] done.")

if __name__ == "__main__":
    main()
