import sqlite3

def get_table_fields(conn, table):
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    return {x[1]: x for x in cur.fetchall()}

def recreate_table_with_fields(db_path, table, required_fields, primary_key='id'):
    """
    如果表缺少主键字段，则自动新建完整结构表并迁移数据
    """
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    # 1. 获取现有字段
    fields = get_table_fields(conn, table)
    # 2. 检查主键
    need_rebuild = False
    if primary_key not in fields:
        print(f"[重建] {db_path}.{table} 缺失主键 {primary_key}，自动迁移重建...")
        need_rebuild = True
    else:
        # 检查主键是不是INTEGER PRIMARY KEY
        col_info = fields[primary_key]
        if col_info[5] != 1:  # PK为1
            print(f"[重建] {db_path}.{table} 主键字段 {primary_key} 定义不正确，自动迁移重建...")
            need_rebuild = True

    # 3. 重建表结构（只在缺主键时）
    if need_rebuild:
        tmp_table = f"{table}_tmp"
        # 构造完整字段定义
        field_defs = []
        for k, v in required_fields.items():
            if k == primary_key:
                field_defs.append(f"{k} INTEGER PRIMARY KEY AUTOINCREMENT")
            else:
                field_defs.append(f"{k} {v}")
        create_sql = f"CREATE TABLE {tmp_table} ({', '.join(field_defs)})"
        cur.execute(create_sql)
        # 迁移老数据（有则迁移对应字段）
        exist_fields = list(fields.keys())
        migrate_fields = [k for k in required_fields if k in exist_fields]
        if migrate_fields:
            cur.execute(f"INSERT INTO {tmp_table} ({', '.join(migrate_fields)}) SELECT {', '.join(migrate_fields)} FROM {table}")
        # 删除旧表，rename新表
        cur.execute(f"DROP TABLE {table}")
        cur.execute(f"ALTER TABLE {tmp_table} RENAME TO {table}")
        print(f"[重建] {db_path}.{table} 主键补全+字段修复完成。")
    # 4. 再补所有缺字段（正常字段）
    ensure_table_fields(db_path, table, required_fields)
    conn.close()

def ensure_table_fields(db_path, table, required_fields: dict):
    """
    补全普通字段（不处理主键，主键需用recreate_table_with_fields）
    """
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    columns = [x[1] for x in cur.fetchall()]
    for field, sql in required_fields.items():
        if field not in columns and field != 'id':
            try:
                cur.execute(f"ALTER TABLE {table} ADD COLUMN {field} {sql}")
                print(f"[升级] {db_path}.{table} 自动补字段: {field}")
            except Exception as e:
                print(f"[警告] {db_path}.{table} 字段 {field} 补齐失败: {e}")
    conn.commit()
    conn.close()

# === 配置你主要表的必需字段 ===

required_tables = {
    "strategy_pool.db": {
        "strategies": {
            "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
            "symbol": "TEXT",
            "group_name": "TEXT",
            "params": "TEXT",
            "score": "REAL",
            "status": "TEXT DEFAULT 'active'",
            "pool_layer": "TEXT",
            "created_at": "TEXT"
        },
        "observe_pool": {
            "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
            "symbol": "TEXT",
            "group_name": "TEXT",
            "params": "TEXT",
            "score": "REAL",
            "created_at": "TEXT"
        }
    },
    "ai_params.db": {
        "ai_params": {
            "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
            "param_group_id": "INTEGER",
            "params": "TEXT",
            "score": "REAL",
            "status": "TEXT DEFAULT 'active'",
            "created_at": "TEXT"
        }
    },
    "simu_trades.db": {
        "simu_trades": {
            "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
            "instId": "TEXT",
            "price": "REAL",
            "vol": "REAL",
            "status": "TEXT",
            "param_group_id": "INTEGER",
            "ts": "TEXT"
        }
    },
    "trades.db": {
        "trades": {
            "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
            "instId": "TEXT",
            "price": "REAL",
            "vol": "REAL",
            "status": "TEXT",
            "param_group_id": "INTEGER",
            "ts": "TEXT"
        }
    },
    # 可继续加其他关键表
}

def upgrade_all_tables(base_dir):
    for dbfile, tables in required_tables.items():
        db_path = f"{base_dir}/{dbfile}"
        for tablename, fields in tables.items():
            recreate_table_with_fields(db_path, tablename, fields, primary_key="id")
    print("[表结构升级自愈] 全部主表已自动补全。")

# === 用法 ===
# upgrade_all_tables(r'D:/ai/okx_bot/data/dbs')

