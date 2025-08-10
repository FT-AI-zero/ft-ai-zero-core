import sqlite3
import datetime
import os
import json
import traceback

from ailearning.ai_engine import ai_risk_decision, merge_full_template, load_ai_pool
from utils.config import TRADES_DB, SIMU_TRADES_DB, AI_PARAMS_DB
from utils.db_upgrade import ensure_table_fields

# ====== 字段模板 ======
SIMULATION_REQUIRED_FIELDS = {
    "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
    "instId": "TEXT",
    "price": "REAL",
    "vol": "REAL",
    "side": "TEXT",
    "ai_score": "REAL",
    "ai_pass": "INTEGER",
    "ai_reason": "TEXT",
    "status": "TEXT",
    "strategy_id": "TEXT",
    "ai_params_json": "TEXT",
    "sim_time": "TEXT",
    "param_group_id": "INTEGER",
    "param_group_score": "REAL",
    "param_group_win_rate": "REAL"
}
TRADES_REQUIRED_FIELDS = {
    "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
    "instId": "TEXT",
    "ts": "INTEGER",
    "price": "REAL",
    "vol": "REAL",
    "status": "TEXT",
    "strategy_id": "TEXT",
    "comment": "TEXT",
    "meta": "TEXT"
}

def get_trades_db_conn():
    return sqlite3.connect(TRADES_DB)

def get_simu_db_conn():
    return sqlite3.connect(SIMU_TRADES_DB)

def fetch_trades_for_simulation(limit=300):
    ensure_table_fields(TRADES_DB, "trades", TRADES_REQUIRED_FIELDS)
    conn = get_trades_db_conn()
    c = conn.cursor()
    print(f"[数据读取] 尝试读取最近 {limit} 条交易数据 (trades.db)")
    try:
        c.execute("SELECT * FROM trades ORDER BY ts DESC LIMIT ?", (limit,))
        fields = [col[0] for col in c.description]
        rows = [dict(zip(fields, row)) for row in c.fetchall()]
    except Exception as e:
        print(f"[数据读取错误] {e}")
        rows = []
    conn.close()
    return rows

def fetch_kline_price(instId, ts, bar="1m"):
    import os
    from utils.config import DB_DIR
    dbfile = os.path.join(DB_DIR, f'kline_{bar}.db')
    try:
        conn = sqlite3.connect(dbfile)
        c = conn.cursor()
        c.execute(
            f"SELECT close FROM kline_{bar} WHERE instId=? AND ts<=? ORDER BY ts DESC LIMIT 1",
            (instId, ts)
        )
        row = c.fetchone()
        conn.close()
        if row:
            return float(row[0])
    except Exception as e:
        print(f"[KLINE读取异常]{instId}@{ts}: {e}")
    return None

def ensure_simulation_results_table():
    ensure_table_fields(SIMU_TRADES_DB, "simulation_results", SIMULATION_REQUIRED_FIELDS)
    print("[建表] simulation_results 表创建/字段自动补全完成")

def run_simulation(limit=300, score_threshold=0.5):
    trades = fetch_trades_for_simulation(limit)
    if not trades:
        print("[仿真] 无交易数据，退出")
        return

    ai_param_groups = load_ai_pool(min_win_rate=0.6, min_score=6.5, top_k=5)
    if not ai_param_groups:
        print("[仿真] 没有高胜率参数组，自动切换为最高分参数组")
        ai_param_groups = load_ai_pool(min_win_rate=0, min_score=0, top_k=1)
    print(f"[仿真] 本轮共用 {len(ai_param_groups)} 组AI参数做回测")

    ensure_simulation_results_table()
    conn = get_simu_db_conn()
    c = conn.cursor()

    for params_group in ai_param_groups:
        params = params_group["params"]
        group_id = params_group.get("id")
        group_score = params_group.get("score")
        group_win_rate = params_group.get("win_rate")
        inserted_count = 0
        print(f"\n[仿真] 正在用AI参数组ID={group_id} (score={group_score}, win_rate={group_win_rate}) 做批量回测")

        for t in trades:
            try:
                ts_val = None
                if "ts" in t:
                    try:
                        if isinstance(t["ts"], int) or isinstance(t["ts"], float):
                            ts_val = int(t["ts"])
                        else:
                            ts_val = int(datetime.datetime.fromisoformat(t["ts"]).timestamp())
                    except Exception:
                        ts_val = None

                kline_price = fetch_kline_price(t.get("instId", ""), ts_val, bar="1m") if ts_val else None
                t["kline_close"] = kline_price

                result = ai_risk_decision(t, params=params, mode="all")
                ai_score = result.get("risk_score", 0.5)

                def safe_float(x):
                    try:
                        if x in (None, "", "None"):
                            return 0
                        return float(x)
                    except Exception:
                        return 0

                sim = {
                    "instId": str(t.get("instId", "")),
                    "price": safe_float(t.get("price") or t.get("px")),
                    "vol": safe_float(t.get("vol") or t.get("sz")),
                    "side": str(t.get("side") or t.get("comment") or ""),
                    "ai_score": ai_score,
                    "ai_pass": int(result.get("pass", True)),
                    "ai_reason": str(result.get("reason", "")),
                    "status": str(t.get("status", "")),
                    "strategy_id": str(t.get("strategy_id", "")),
                    "ai_params_json": json.dumps(params, ensure_ascii=False),
                    "sim_time": datetime.datetime.now().isoformat(),
                    "param_group_id": group_id,
                    "param_group_score": group_score,
                    "param_group_win_rate": group_win_rate
                }
                c.execute("""
                    INSERT INTO simulation_results
                    (instId, price, vol, side, ai_score, ai_pass, ai_reason, status, strategy_id, ai_params_json, sim_time, param_group_id, param_group_score, param_group_win_rate)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    sim["instId"], sim["price"], sim["vol"], sim["side"],
                    sim["ai_score"], sim["ai_pass"], sim["ai_reason"],
                    sim["status"], sim["strategy_id"], sim["ai_params_json"],
                    sim["sim_time"], sim["param_group_id"], sim["param_group_score"], sim["param_group_win_rate"]
                ))
                inserted_count += 1
            except Exception as e:
                print(f"[仿真异常] 交易ID {t.get('id', '')} 失败: {e}")
                traceback.print_exc()
        conn.commit()
        print(f"[仿真完成] 参数组ID={group_id} 已写入 {inserted_count} 条 simulation_results 记录")

    conn.close()
    print(f"[仿真] 所有AI参数组回测完成！")
    sync_simulation_performance_to_ai_params()


def sync_simulation_performance_to_ai_params():
    """闭环：回测绩效同步回 ai_params 参数池"""
    try:
        conn = sqlite3.connect(SIMU_TRADES_DB)
        c = conn.cursor()
        sql = """
        SELECT param_group_id, 
               AVG(ai_score) AS avg_score,
               SUM(CASE WHEN ai_pass=1 THEN 1 ELSE 0 END)*1.0/COUNT(*) AS win_rate,
               COUNT(*) AS trade_count
          FROM simulation_results
         GROUP BY param_group_id
        """
        perf = {}
        for row in c.execute(sql):
            pid, avg_score, win_rate, trade_count = row
            perf[pid] = {
                "avg_score": avg_score,
                "win_rate": win_rate,
                "trade_count": trade_count
            }
        c.close()
        conn.close()
        # 写回 ai_params
        conn2 = sqlite3.connect(AI_PARAMS_DB)
        c2 = conn2.cursor()
        for pid, metrics in perf.items():
            c2.execute("UPDATE ai_params SET win_rate=?, score=?, trade_count=? WHERE id=?",
                (metrics["win_rate"], metrics["avg_score"], metrics["trade_count"], pid))
        conn2.commit()
        conn2.close()
        print(f"[同步] 回测绩效已写回 ai_params 表，共更新 {len(perf)} 组参数")
    except Exception as e:
        print(f"[同步异常] 回测绩效同步失败: {e}")

if __name__ == "__main__":
    run_simulation(limit=300, score_threshold=0.5)
