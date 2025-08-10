# tools/migrate_allowlist_unique_fix.py
import sqlite3
from pathlib import Path
from utils.config import DATA_DIR, MODE

def _sp_db_paths():
    base = Path(DATA_DIR)
    root = base.parent if base.name in ("shared", "paper", "live") else base
    # 两个环境都修一遍（存在就修）
    return [
        str(root / "paper" / "dbs" / "strategy_pool.db"),
        str(root / "live"  / "dbs" / "strategy_pool.db"),
    ]

DDL_NEW = """
CREATE TABLE IF NOT EXISTS allowlist(
    param_group_id INTEGER NOT NULL,
    "window"       TEXT    NOT NULL,
    score          REAL    NOT NULL,
    trades         INTEGER NOT NULL,
    source         TEXT    DEFAULT '',
    weight         REAL    DEFAULT 1.0,
    updated_at     TEXT
);
"""

IDX_NEW = """CREATE UNIQUE INDEX IF NOT EXISTS ux_allowlist_pg_win
             ON allowlist(param_group_id, "window");"""

def table_exists(con, name:str)->bool:
    return con.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?;", (name,)).fetchone()[0] > 0

def col_exists(con, table:str, col:str)->bool:
    return any(r[1] == col for r in con.execute(f"PRAGMA table_info({table});").fetchall())

def migrate(db_path:str):
    p = Path(db_path)
    if not p.exists():
        print(f"[skip] {db_path} (not exists)")
        return

    con = sqlite3.connect(db_path)
    cur = con.cursor()

    if not table_exists(con, "allowlist"):
        # 没表就直接创建新表
        cur.executescript(DDL_NEW)
        cur.executescript(IDX_NEW)
        con.commit()
        con.close()
        print(f"[ok] created fresh allowlist -> {db_path}")
        return

    # 把旧表重命名
    cur.execute("ALTER TABLE allowlist RENAME TO allowlist_old;")

    # 创建新表 & 复合唯一索引
    cur.executescript(DDL_NEW)
    cur.executescript(IDX_NEW)

    # 兼容旧表是否有 window/weight/source 等字段
    has_window = col_exists(con, "allowlist_old", "window")
    has_weight = col_exists(con, "allowlist_old", "weight")
    has_source = col_exists(con, "allowlist_old", "source")

    src_window = '"window"' if has_window else "'7d'"
    src_weight = "weight"    if has_weight else "1.0"
    src_source = "source"    if has_source else "''"

    # 去重拷贝（以 pg, window 聚合，保最新/分高可以之后再做，这里只做结构迁移）
    cur.execute(f"""
        INSERT OR IGNORE INTO allowlist(param_group_id, "window", score, trades, source, weight, updated_at)
        SELECT
            param_group_id,
            {src_window}    AS "window",
            score,
            trades,
            {src_source}     AS source,
            {src_weight}     AS weight,
            updated_at
          FROM allowlist_old
      GROUP BY param_group_id, "window";
    """)

    # 可以清掉旧唯一索引（如果旧结构有），直接删表就顺带移除了
    cur.execute("DROP TABLE allowlist_old;")

    con.commit()
    con.close()
    print(f"[ok] migrated allowlist -> {db_path}")

def main():
    for db in _sp_db_paths():
        migrate(db)

if __name__ == "__main__":
    main()
