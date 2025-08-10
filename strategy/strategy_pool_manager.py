import os
import sqlite3
import datetime
import json
import traceback
from utils.config import STRATEGY_POOL_DB, NOSTRATEGY_POOL_DB, DB_DIR

def ensure_signals_table(conn):
    """确保 signals 表存在，且结构完整"""
    try:
        c = conn.cursor()
        c.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            instId TEXT,
            period TEXT,
            ts INTEGER,
            signal_type TEXT,
            status TEXT DEFAULT 'WAIT',
            score REAL,
            params TEXT,
            created_at TEXT
        )
        """)
        # 自动补字段（以后可按需扩展）
        required_fields = {
            "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
            "instId": "TEXT",
            "period": "TEXT",
            "ts": "INTEGER",
            "signal_type": "TEXT",
            "status": "TEXT DEFAULT 'WAIT'",
            "score": "REAL",
            "params": "TEXT",
            "created_at": "TEXT",
        }
        c.execute("PRAGMA table_info(signals)")
        existing_fields = set([x[1] for x in c.fetchall()])
        for field, field_type in required_fields.items():
            if field not in existing_fields:
                print(f"[升级] 补充 signals 表缺失字段: {field}")
                c.execute(f"ALTER TABLE signals ADD COLUMN {field} {field_type}")
        conn.commit()
        print("[建表] signals 表创建或升级成功")
        return True
    except Exception as e:
        print(f"[建表异常] signals 表创建或升级失败: {e}")
        traceback.print_exc()
        return False

def export_to_signal_db():
    """
    从 Active 与 Observe 策略池数据库批量导入信号到 signal_pool.db。
    Active 池：strategy_pool.db 的 strategies 表
    Observe 池：nostrategy_pool.db 的 strategies 表
    """
    signal_db = os.path.join(DB_DIR, "signal_pool.db")

    try:
        conn = sqlite3.connect(signal_db)
    except Exception as e:
        print(f"[连接异常] 连接 signal_pool.db 失败: {e}")
        return

    if not ensure_signals_table(conn):
        conn.close()
        return

    inserted = 0
    # 导入 Active 池
    try:
        sconn = sqlite3.connect(STRATEGY_POOL_DB)
        sc = sconn.cursor()
        # 你的主策略池表名为 strategies，字段见你的实际db
        sc.execute("""
            SELECT symbol, group_name, last_eval_time, pool_layer, score, params
            FROM strategies
        """)
        rows = sc.fetchall()
        print(f"[查询] Active池策略数量: {len(rows)}")
        for row in rows:
            if len(row) == 6:
                symbol, group, ut, pool_layer, score, params_data = row
                try:
                    ts = int(datetime.datetime.fromisoformat(ut).timestamp()) if ut else 0
                except Exception:
                    ts = 0
                created = ut or datetime.datetime.now().isoformat()
                status_col = pool_layer or "active"
            else:
                print(f"[警告] Active池策略数据行字段数异常: {row}")
                continue
            params_str = params_data if isinstance(params_data, str) else json.dumps(params_data, ensure_ascii=False)
            try:
                conn.execute("""
                    INSERT INTO signals
                    (instId, period, ts, signal_type, status, score, params, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    symbol,
                    group,
                    ts,
                    status_col,  # signal_type存状态，如active/eliminated
                    'WAIT',
                    score or 0,
                    params_str,
                    created
                ))
                inserted += 1
            except Exception as e:
                print(f"[插入异常] Active池信号插入失败: {e}")
                traceback.print_exc()
        sconn.close()
        print(f"[导入] Active池策略信号导入成功，条数: {inserted}")
    except Exception as e:
        print(f"[导入异常] Active池信号导入失败: {e}")
        traceback.print_exc()

    # 导入 Observe 池
    try:
        sconn2 = sqlite3.connect(NOSTRATEGY_POOL_DB)
        sc2 = sconn2.cursor()
        sc2.execute("""
            SELECT symbol, group_name, last_eval_time, 'OBSERVE' AS status_col, score, params
            FROM strategies
        """)
        rows2 = sc2.fetchall()
        print(f"[查询] Observe池策略数量: {len(rows2)}")
        for row in rows2:
            if len(row) == 6:
                symbol, group, ut, status_col, score, params_data = row
                try:
                    ts = int(datetime.datetime.fromisoformat(ut).timestamp()) if ut else 0
                except Exception:
                    ts = 0
                created = ut or datetime.datetime.now().isoformat()
            else:
                print(f"[警告] Observe池策略数据行字段数异常: {row}")
                continue
            params_str = params_data if isinstance(params_data, str) else json.dumps(params_data, ensure_ascii=False)
            try:
                conn.execute("""
                    INSERT INTO signals
                    (instId, period, ts, signal_type, status, score, params, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    symbol,
                    group,
                    ts,
                    status_col,
                    'WAIT',
                    score or 0,
                    params_str,
                    created
                ))
                inserted += 1
            except Exception as e:
                print(f"[插入异常] Observe池信号插入失败: {e}")
                traceback.print_exc()
        sconn2.close()
        print(f"[导入] Observe池策略信号导入成功，条数: {inserted}")
    except Exception as e:
        print(f"[导入异常] Observe池信号导入失败: {e}")
        traceback.print_exc()

    # 提交事务
    try:
        conn.commit()
        print(f"[完成] 已批量导入信号 {inserted} 条到 signal_pool.db")
    except Exception as e:
        print(f"[提交异常] 数据提交失败: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    export_to_signal_db()
