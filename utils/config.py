# utils/config.py  —— 统一命名空间 + 全量兼容变量 + SQLite 兼容工具
import os
import sqlite3
from pathlib import Path
from typing import Optional

# ===== 运行模式（paper / live）=====
MODE = os.environ.get("FT_MODE", "paper").strip().lower()
if MODE not in {"paper", "live"}:
    MODE = "paper"

# ===== 项目根 =====
BASE_DIR = Path(__file__).resolve().parents[1]

# ===== 数据目录 =====
DATA_DIR   = BASE_DIR / "data"
NS_DIR     = DATA_DIR / MODE          # data/paper 或 data/live
SHARED_DIR = DATA_DIR / "shared"

# 命名空间内
DB_DIR    = NS_DIR / "dbs"
LOG_DIR   = NS_DIR / "logs"
JSON_DIR  = NS_DIR / "jsons"
MODEL_DIR = NS_DIR / "models"

# 共享区
SHARED_DB_DIR   = SHARED_DIR / "dbs"
SHARED_JSON_DIR = SHARED_DIR / "jsons"

def ensure_dirs():
    for p in [
        DB_DIR, LOG_DIR, JSON_DIR, MODEL_DIR,
        SHARED_DB_DIR, SHARED_JSON_DIR,
        DATA_DIR / "runtime"
    ]:
        p.mkdir(parents=True, exist_ok=True)

ensure_dirs()

# ===== 核心 DB 路径 =====
TRADES_DB          = DB_DIR / "trades.db"
SIMU_TRADES_DB     = DB_DIR / "simu_trades.db"      # ← 模拟盘成交库（老代码需要）
REVIEW_DB          = DB_DIR / "review.db"
SIGNALS_DB         = DB_DIR / "signals.db"
STRATEGY_POOL_DB   = DB_DIR / "strategy_pool.db"
NOSTRATEGY_POOL_DB = DB_DIR / "nostrategy_pool.db"
FEATURES_DB        = DB_DIR / "features.db"
KLINE_DB           = DB_DIR / "kline.db"            # 若拆分多周期，可在采集器里统一写到这里
AI_PARAMS_DB       = SHARED_DB_DIR / "ai_params.db" # shared 共用

# ===== 向后兼容别名（老代码仍可用）=====
SIGNAL_POOL_DB   = SIGNALS_DB
STRATEGY_DB      = STRATEGY_POOL_DB
SIMU_DB          = SIMU_TRADES_DB

# ===== 常用日志文件（覆盖老变量名）=====
TRADE_ENGINE_LOG      = LOG_DIR / "trade_engine.log"
ZERO_LOG              = LOG_DIR / "zero_engine.log"
HEALTH_LOG            = LOG_DIR / "health.log"
POSITION_GUARD_LOG    = LOG_DIR / "position_guard.log"
POSITION_MANAGER_LOG  = LOG_DIR / "position_manager.log"
PM_AUTO_TUNER_LOG     = LOG_DIR / "pm_auto_tuner.log"
PIPELINE_LOG          = LOG_DIR / "pipeline.log"
SCHEDULER_LOG         = LOG_DIR / "scheduler.log"
COLLECTOR_LOG         = LOG_DIR / "collector.log"
INTEL_COLLECTOR_LOG   = LOG_DIR / "intel_collector.log"
SIGNAL_GEN_LOG        = LOG_DIR / "signal_generator.log"
ORDER_ATTEMPTS_LOG    = LOG_DIR / "order_attempts.log"

# ===== OKX 环境 =====


# ===== SQLite 兼容工具 =====
def has_table(db_path: Path, table: str) -> bool:
    dbp = Path(db_path)
    if not dbp.exists():
        return False
    conn = sqlite3.connect(dbp)
    try:
        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table,)
        )
        return cur.fetchone() is not None
    finally:
        conn.close()

def has_column(db_path: Path, table: str, column: str) -> bool:
    if not has_table(db_path, table):
        return False
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.execute(f"PRAGMA table_info({table})")
        return any(r[1] == column for r in cur.fetchall())
    finally:
        conn.close()

def add_column_if_missing(
    db_path: Path, table: str, column: str, ddl_type: str, default_sql: Optional[str] = None
):
    """
    ddl_type 例如: 'REAL', 'INTEGER', 'TEXT'
    default_sql 示例: '0', 'NULL', "''"
    """
    if not has_table(db_path, table):
        return
    if has_column(db_path, table, column):
        return
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl_type}")
        if default_sql is not None:
            conn.execute(f"UPDATE {table} SET {column}={default_sql}")
        conn.commit()
    finally:
        conn.close()

def current_mode() -> str:
    return MODE
