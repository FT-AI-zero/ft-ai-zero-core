import sqlite3
import datetime
import os
import json
from utils.config import STRATEGY_POOL_DB, DB_DIR

# 迁移参数
SCORE_UPGRADE = 8.0
SCORE_DOWNGRADE = 6.0
SCORE_ELIMINATE = 4.0
MAX_ACTIVE = 150
MAX_WATCH = 300

def ensure_strategy_table():
    """保证 pool_layer 字段和 update_time 字段存在"""
    conn = sqlite3.connect(STRATEGY_POOL_DB)
    c = conn.cursor()
    # 原有表结构补字段
    c.execute("""
        CREATE TABLE IF NOT EXISTS strategies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            group_name TEXT,
            params TEXT,
            score REAL DEFAULT 0,
            last_eval_time TEXT,
            pool_layer TEXT DEFAULT 'candidate',
            update_time TEXT
        )
    """)
    # 自动补字段
    for col, typ, dft in [
        ("pool_layer", "TEXT", "'candidate'"),
        ("update_time", "TEXT", "NULL")
    ]:
        c.execute("PRAGMA table_info(strategies)")
        fields = [row[1] for row in c.fetchall()]
        if col not in fields:
            print(f"[升级] 自动补全 strategies 字段: {col}")
            c.execute(f"ALTER TABLE strategies ADD COLUMN {col} {typ} DEFAULT {dft}")
    conn.commit()
    conn.close()

def db_conn():
    return sqlite3.connect(STRATEGY_POOL_DB)

def archive_migration_log(logdict):
    """归档每次迁移结果，便于复盘"""
    archive_path = os.path.join(DB_DIR, "strategy_pool_migration.log")
    with open(archive_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(logdict, ensure_ascii=False) + "\n")
    print(f"[归档] 迁移日志已写入 {archive_path}")

def migrate_pools():
    ensure_strategy_table()
    conn = db_conn()
    c = conn.cursor()

    up_ids, down_ids, elim_ids, to_history_ids = [], [], [], []

    # 晋级：候选->活跃
    c.execute("UPDATE strategies SET pool_layer='active', update_time=? WHERE pool_layer='candidate' AND score>=?", 
              (datetime.datetime.now().isoformat(), SCORE_UPGRADE))
    up = c.rowcount
    if up:
        c.execute("SELECT id FROM strategies WHERE pool_layer='active' AND score>=?", (SCORE_UPGRADE,))
        up_ids = [row[0] for row in c.fetchall()]

    # 降级：活跃->观察
    c.execute("UPDATE strategies SET pool_layer='watch', update_time=? WHERE pool_layer='active' AND score<? AND score>=?", 
              (datetime.datetime.now().isoformat(), SCORE_DOWNGRADE, SCORE_ELIMINATE))
    down = c.rowcount
    if down:
        c.execute("SELECT id FROM strategies WHERE pool_layer='watch' AND score<? AND score>=?", (SCORE_DOWNGRADE, SCORE_ELIMINATE))
        down_ids = [row[0] for row in c.fetchall()]

    # 淘汰（任意池，分数极低）
    for layer in ['candidate', 'active', 'watch']:
        c.execute(f"UPDATE strategies SET pool_layer='eliminated', update_time=? WHERE pool_layer='{layer}' AND score<?", 
                  (datetime.datetime.now().isoformat(), SCORE_ELIMINATE))
        c.execute(f"SELECT id FROM strategies WHERE pool_layer='eliminated' AND score<?", (SCORE_ELIMINATE,))
        elim_ids += [row[0] for row in c.fetchall()]

    # 控制池内最大数量，多余的转历史池
    for layer, max_count in [('active', MAX_ACTIVE), ('watch', MAX_WATCH)]:
        c.execute(f"SELECT id FROM strategies WHERE pool_layer=? ORDER BY update_time DESC", (layer,))
        ids = [row[0] for row in c.fetchall()]
        if len(ids) > max_count:
            for id_ in ids[max_count:]:
                c.execute("UPDATE strategies SET pool_layer='history', update_time=? WHERE id=?", 
                          (datetime.datetime.now().isoformat(), id_))
                to_history_ids.append(id_)

    conn.commit()
    print(f"[迁移] 晋级活跃:{up}，降级观察:{down}，淘汰:{len(elim_ids)}，转历史:{len(to_history_ids)}。")
    # 归档迁移结果
    archive_migration_log({
        "ts": datetime.datetime.now().isoformat(),
        "up_ids": up_ids,
        "down_ids": down_ids,
        "elim_ids": elim_ids,
        "to_history_ids": to_history_ids
    })
    conn.close()

if __name__ == "__main__":
    migrate_pools()
