# tools/db_migrator.py
import sqlite3
from pathlib import Path
from utils.config import (
    TRADES_DB, REVIEW_DB, SIGNALS_DB, STRATEGY_POOL_DB, NOSTRATEGY_POOL_DB,
    FEATURES_DB, KLINE_DB, AI_PARAMS_DB,
    add_column_if_missing, has_table
)

MIGRATIONS = [
    # (db, table, column, ddl_type, default_sql)
    (TRADES_DB, "trades", "notional", "REAL", "NULL"),
    (TRADES_DB, "trades", "pnl_ratio", "REAL", "NULL"),
    (TRADES_DB, "trades", "vol", "REAL", "NULL"),

    (REVIEW_DB, "review", "pnl_ratio", "REAL", "NULL"),
    (REVIEW_DB, "review", "vol", "REAL", "NULL"),
]

def run():
    touched = 0
    for db, table, col, ddl, default_sql in MIGRATIONS:
        if has_table(db, table):
            add_column_if_missing(db, table, col, ddl, default_sql)
            touched += 1
    print(f"[migrator] done. touched={touched}")

if __name__ == "__main__":
    run()
