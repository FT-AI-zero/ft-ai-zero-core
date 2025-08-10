# collectors/feature_engine.py
import os
import sqlite3
from pathlib import Path
from typing import List, Tuple, Optional

import numpy as np
import pandas as pd

from utils.config import DB_DIR, FEATURES_DB
try:
    from utils.db_upgrade import ensure_table_fields
except Exception:
    def ensure_table_fields(*args, **kwargs):  # 兼容无此工具
        pass

# ===== 可调参数（也可用环境变量覆盖）=====
BATCH_SIZE = int(os.environ.get("FE_BATCH", "200"))
ROW_LIMIT  = int(os.environ.get("FE_ROWS",  "500"))   # 每个 instId 读取 K 线条数
CYCLE_ONLY = os.environ.get("FE_CYCLE", "").strip()   # 例如 "15m"，空则全部周期
PER_TABLE_LIMIT = int(os.environ.get("FE_LIMIT", "0"))  # 每张表最多处理多少 instId，0=不限

# ===== 指标字段 =====
FEATURE_FIELDS = {
    "instId": "TEXT", "ts": "INTEGER",
    "ma5": "REAL", "ma10": "REAL", "ma20": "REAL", "ma30": "REAL",
    "ema7": "REAL", "ema25": "REAL",
    "boll_ma": "REAL", "boll_upper": "REAL", "boll_lower": "REAL",
    "rsi6": "REAL", "rsi14": "REAL",
    "k": "REAL", "d": "REAL", "j": "REAL",
    "macd": "REAL", "macds": "REAL", "macdh": "REAL",
    "atr14": "REAL"
}

# ===== 基础指标 =====
def MA(series, n):  return series.rolling(window=n).mean()
def EMA(series, n): return series.ewm(span=n, adjust=False).mean()
def BOLL(series, n=20):
    ma = MA(series, n); std = series.rolling(window=n).std()
    return ma, ma + 2*std, ma - 2*std

def RSI(series, n=14):
    diff = series.diff()
    gain = (diff.where(diff > 0, 0)).rolling(n).mean()
    loss = (-diff.where(diff < 0, 0)).rolling(n).mean()
    rs = gain / (loss + 1e-9)
    return 100 - (100 / (1 + rs))

def KDJ(df, n=9):
    low = df['low'].rolling(n).min(); high = df['high'].rolling(n).max()
    rsv = (df['close'] - low) / (high - low + 1e-9) * 100
    K = rsv.ewm(com=2).mean(); D = K.ewm(com=2).mean(); J = 3*K - 2*D
    return K, D, J

def MACD(series, fast=12, slow=26, signal=9):
    ema_fast = EMA(series, fast); ema_slow = EMA(series, slow)
    macd = ema_fast - ema_slow; sig = EMA(macd, signal); hist = macd - sig
    return macd, sig, hist

def ATR(df, n=14):
    hl = df['high'] - df['low']
    hc = (df['high'] - df['close'].shift()).abs()
    lc = (df['low']  - df['close'].shift()).abs()
    tr = pd.concat([hl, hc, lc], axis=1).max(axis=1)
    return tr.rolling(n).mean()

# ===== IO / 扫描 =====
def list_all_kline_sources() -> List[Tuple[Path, str]]:
    res: List[Tuple[Path, str]] = []
    for p in Path(DB_DIR).glob("kline_*.db"):
        with sqlite3.connect(p) as conn:
            rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'kline_%'").fetchall()
            for (tname,) in rows:
                if CYCLE_ONLY and not tname.endswith(CYCLE_ONLY):
                    continue
                res.append((p, tname))
    return res

def _conn_fast(db_path: Path):
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=OFF;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    return conn

def ensure_feature_table(feature_db: Path, feature_table: str, _cache={}):
    feature_db = Path(feature_db)
    feature_db.parent.mkdir(parents=True, exist_ok=True)
    key = f"{feature_db}:{feature_table}"
    if _cache.get(key):  # 已创建过
        return
    conn = _conn_fast(feature_db)
    c = conn.cursor()
    field_sql = ", ".join([f"{k} {v}" for k, v in FEATURE_FIELDS.items()])
    c.execute(f"""
        CREATE TABLE IF NOT EXISTS {feature_table} (
            {field_sql},
            PRIMARY KEY (instId, ts)
        )
    """)
    c.execute(f"CREATE INDEX IF NOT EXISTS idx_{feature_table}_inst ON {feature_table}(instId)")
    c.execute(f"CREATE INDEX IF NOT EXISTS idx_{feature_table}_ts   ON {feature_table}(ts)")
    conn.commit()
    ensure_table_fields(feature_db, feature_table, FEATURE_FIELDS)
    _cache[key] = True
    conn.close()

def fetch_kline_df(kline_db: Path, kline_table: str, instId: str, limit: int = ROW_LIMIT) -> Optional[pd.DataFrame]:
    if not Path(kline_db).exists(): return None
    conn = sqlite3.connect(kline_db)
    try:
        q = f"SELECT ts, open, high, low, close, vol FROM {kline_table} WHERE instId=? ORDER BY ts ASC LIMIT ?"
        df = pd.read_sql(q, conn, params=(instId, limit))
    finally:
        conn.close()
    if df.empty: return None
    df['ts'] = df['ts'].astype(int)
    df.set_index('ts', inplace=True)
    return df

def save_feature_row(feature_db: Path, feature_table: str, instId: str, ts: int, features: dict, _cache={}):
    conn = _cache.get(str(feature_db))
    if conn is None:
        conn = _conn_fast(feature_db)
        _cache[str(feature_db)] = conn
        _cache["cnt"] = 0
    c = conn.cursor()
    keys = ["instId", "ts"] + list(features.keys())
    vals = [instId, ts] + list(features.values())
    q = f"INSERT OR REPLACE INTO {feature_table} ({','.join(keys)}) VALUES ({','.join(['?']*len(keys))})"
    c.execute(q, vals)
    _cache["cnt"] += 1
    if _cache["cnt"] % BATCH_SIZE == 0:
        conn.commit()

def flush_all(_cache=globals()):
    for k, v in list(_cache.items()):
        if isinstance(v, sqlite3.Connection):
            try:
                v.commit(); v.close()
            except: pass
    if "cnt" in _cache: _cache.pop("cnt", None)

# ===== 主逻辑 =====
def compute_and_save_features_for_one(kline_db: Path, kline_table: str, feature_db: Path, feature_table: str, instId: str):
    df = fetch_kline_df(kline_db, kline_table, instId)
    if df is None or len(df) < 30:
        print(f"[跳过] {instId} 数据不足或为空")
        return
    ensure_feature_table(feature_db, feature_table)

    try:
        df['ma5']  = MA(df['close'], 5);   df['ma10'] = MA(df['close'], 10)
        df['ma20'] = MA(df['close'], 20);  df['ma30'] = MA(df['close'], 30)
        df['ema7'] = EMA(df['close'], 7);  df['ema25'] = EMA(df['close'], 25)
        boll_ma, boll_up, boll_lo = BOLL(df['close'])
        df['boll_ma'] = boll_ma; df['boll_upper'] = boll_up; df['boll_lower'] = boll_lo
        df['rsi6']  = RSI(df['close'], 6); df['rsi14'] = RSI(df['close'], 14)
        k, d, j = KDJ(df); df['k'] = k; df['d'] = d; df['j'] = j
        macd, ms, mh = MACD(df['close']); df['macd'] = macd; df['macds'] = ms; df['macdh'] = mh
        df['atr14'] = ATR(df, 14)
    except Exception as e:
        print(f"[指标计算异常] {instId}: {e}")
        return

    latest = df.iloc[-1]; ts = int(df.index[-1])
    features = {k: (None if k in ('instId','ts') else latest.get(k)) for k in FEATURE_FIELDS}
    try:
        save_feature_row(feature_db, feature_table, instId, ts, features)
    except Exception as e:
        print(f"[写入异常] {instId} @ {ts}: {e}")

def main():
    print("=== Feature/指标引擎启动 ===")
    sources = list_all_kline_sources()
    if not sources:
        print(f"[提示] 未发现 kline_*.db / kline_* 表，请先运行采集器。DB_DIR={DB_DIR}")
        return

    feature_db = Path(FEATURES_DB)
    for kdb, ktable in sources:
        ftable = ktable.replace("kline", "features")
        with sqlite3.connect(kdb) as conn:
            rows = conn.execute(f"SELECT DISTINCT instId FROM {ktable}").fetchall()
            insts = [r[0] for r in rows]

        if not insts:
            print(f"[跳过] {kdb.name}:{ktable} 无 instId")
            continue

        processed = 0
        for inst in insts:
            compute_and_save_features_for_one(kdb, ktable, feature_db, ftable, inst)
            processed += 1
            if PER_TABLE_LIMIT and processed >= PER_TABLE_LIMIT:
                print(f"[限额] {ktable} 达到 FE_LIMIT={PER_TABLE_LIMIT}，跳出该表")
                break

    # 收尾提交
    flush_all()
    print("=== 所有K线表指标批量补齐完毕 ===")

if __name__ == "__main__":
    main()
