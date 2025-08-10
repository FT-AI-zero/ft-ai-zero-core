# jobs/rollup_live_trades.py
import sqlite3
from utils.config import TRADES_DB, REVIEW_DB, ensure_dirs, has_table, has_column, add_column_if_missing

REVIEW_SQL = """
CREATE TABLE IF NOT EXISTS review (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_id TEXT,
    ts TEXT,
    instId TEXT,
    side TEXT,
    posSide TEXT,
    tdMode TEXT,
    lever REAL,
    ordType TEXT,
    px REAL,
    sz REAL,
    notional REAL,
    fee REAL,
    pnl REAL,
    pnl_ratio REAL,
    vol REAL,
    extra TEXT
);
"""

def ensure_review_schema():
    conn = sqlite3.connect(REVIEW_DB)
    try:
        conn.execute(REVIEW_SQL)
        conn.commit()
        # 兜底字段
        add_column_if_missing(REVIEW_DB, "review", "pnl_ratio", "REAL", "NULL")
        add_column_if_missing(REVIEW_DB, "review", "vol", "REAL", "NULL")
    finally:
        conn.close()

def run():
    ensure_dirs()
    ensure_review_schema()

    if not has_table(TRADES_DB, "trades"):
        print("[rollup] trades 表不存在，跳过")
        return

    # 动态探测 trades 列
    conn_t = sqlite3.connect(TRADES_DB)
    conn_r = sqlite3.connect(REVIEW_DB)
    try:
        tcols = {r[1] for r in conn_t.execute("PRAGMA table_info(trades)").fetchall()}

        def col(name, alias=None):
            """若列不存在，返回 NULL as 别名"""
            if name in tcols:
                return name
            return f"NULL AS {alias or name}"

        # trade_id 优先级：id > ordId > clOrdId > rowid
        trade_id_expr = "id" if "id" in tcols else (
            "ordId" if "ordId" in tcols else (
                "clOrdId" if "clOrdId" in tcols else "rowid"
            )
        )

        sel = f"""
        SELECT
            {trade_id_expr} AS trade_id,
            {col('ts')}, {col('instId')}, {col('side')}, {col('posSide')},
            {col('tdMode')}, {col('lever')}, {col('ordType')},
            {col('px')}, {col('sz')}, {col('notional')},
            {col('fee')}, {col('pnl')}, {col('pnl_ratio')},
            {col('vol')}, {col('extra')}
        FROM trades
        """
        rows = list(conn_t.execute(sel))

        # 插入 review
        inserted = 0
        for (trade_id, ts, instId, side, posSide, tdMode, lever, ordType,
             px, sz, notional, fee, pnl, pnl_ratio, vol, extra) in rows:

            # 计算 vol
            if vol is None:
                if notional is not None:
                    vol_calc = abs(notional or 0.0)
                elif px is not None and sz is not None:
                    vol_calc = abs((px or 0.0) * (sz or 0.0))
                else:
                    vol_calc = None
            else:
                vol_calc = vol

            conn_r.execute("""
                INSERT INTO review
                (trade_id, ts, instId, side, posSide, tdMode, lever, ordType,
                 px, sz, notional, fee, pnl, pnl_ratio, vol, extra)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (str(trade_id) if trade_id is not None else None,
                  ts, instId, side, posSide, tdMode, lever, ordType,
                  px, sz, notional, fee, pnl, pnl_ratio, vol_calc, extra))
            inserted += 1

        conn_r.commit()
        print(f"[rollup] matched rows={inserted}")
    finally:
        conn_t.close()
        conn_r.close()

if __name__ == "__main__":
    run()
