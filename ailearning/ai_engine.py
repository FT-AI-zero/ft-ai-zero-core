import sqlite3
import os
import json
import random
import datetime
import traceback
from utils.config import AI_PARAMS_DB

# ------------- 参数模板（含版本号和状态） ---------------
FULL_PARAM_TEMPLATE = {
    "DEFAULT_LEVER": 10,
    "MAX_RISK_SCORE": 0.8,
    "MIN_RISK_SCORE": 0.2,
    "TP_RATE": 0.03,
    "SL_RATE": 0.01,
    "AI_VERSION": "v1.0",
    "MAX_ADD_POS_TIMES": 3,
    "MAX_POSITION_RATIO": 0.25,
    "MAX_LOSS_RATIO": 0.1,
    "RISK_CHECK_INTERVAL": 60,
    "FORCE_CLOSE_RATIO": 0.95,
    "PROTECT_MARGIN_RATIO": 0.3,
    "TRAILING_STOP_RATE": 0.015,
    "MAX_TRADE_PER_DAY": 30,
    "FEE_RATE": 0.0005,
    "STRATEGY_POOL": ["AI_MOMENTUM", "GRID_V2", "MANUAL_OVERRIDE"],
    "DEBUG_MODE": False,
    "type": "trend",
    "risk_level": "medium",
    "timeframe": "mid",
    "volatility": "normal",
    "version": "v1.0",
    "status": "active"
}

def merge_full_template(params):
    merged = FULL_PARAM_TEMPLATE.copy()
    if isinstance(params, dict):
        merged.update(params)
    if "version" not in merged:
        merged["version"] = "v1.0"
    if "status" not in merged:
        merged["status"] = "active"
    return merged

def ensure_ai_params_table():
    try:
        conn = sqlite3.connect(AI_PARAMS_DB)
        cur = conn.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS ai_params (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            params_json TEXT,
            score REAL,
            ts TEXT,
            version TEXT,
            status TEXT
        )
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS ai_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_time TEXT,
            snapshot_json TEXT
        )
        """)
        # 自动补字段，避免缺失导致异常
        required_fields = {
            "win_rate": "REAL DEFAULT 0",
            "profit_rate": "REAL DEFAULT 0",
            "trade_count": "INTEGER DEFAULT 0",
        }
        cur.execute("PRAGMA table_info(ai_params)")
        existing = set([x[1] for x in cur.fetchall()])
        for field, field_type in required_fields.items():
            if field not in existing:
                print(f"[升级] 自动补全 ai_params 字段: {field}")
                cur.execute(f"ALTER TABLE ai_params ADD COLUMN {field} {field_type}")
        conn.commit()
        print("[建表] ai_params 和 ai_snapshots 表已创建或存在，字段自动补全完成")
    except Exception as e:
        print("[错误] 建表失败:", e)
    finally:
        conn.close()

class AiParamsRepository:
    def __init__(self, db_path=AI_PARAMS_DB):
        self.db_path = db_path
    def _connect(self):
        return sqlite3.connect(self.db_path)

    def load_all(self, status_filter="active", version_filter=None):
        conn = self._connect()
        c = conn.cursor()
        pool = []
        try:
            sql = "SELECT id, params_json, score, ts, version, status, win_rate, profit_rate, trade_count FROM ai_params WHERE status=?"
            params = [status_filter]
            if version_filter:
                sql += " AND version=?"
                params.append(version_filter)
            c.execute(sql, params)
            for row in c.fetchall():
                (rid, params_json, score, ts, version, status, win_rate, profit_rate, trade_count) = row
                params_dict = json.loads(params_json)
                params_dict = merge_full_template(params_dict)
                pool.append({
                    "id": rid,
                    "params": params_dict,
                    "score": score,
                    "ts": ts,
                    "version": version,
                    "status": status,
                    "win_rate": win_rate,
                    "profit_rate": profit_rate,
                    "trade_count": trade_count
                })
        finally:
            conn.close()
        return pool

    def save_all(self, pool):
        try:
            conn = self._connect()
            c = conn.cursor()
            c.execute("DELETE FROM ai_params")
            for item in pool:
                params = merge_full_template(item.get("params", {}))
                version = params.get("version", "v1.0")
                status = params.get("status", "active")
                params_json = json.dumps(params, ensure_ascii=False)
                score = item.get("score") or 0
                ts_val = item.get("ts") or datetime.datetime.now().isoformat()
                win_rate = item.get("win_rate", 1.0)
                profit_rate = item.get("profit_rate", 0.0)
                trade_count = item.get("trade_count", 0)
                c.execute(
                    "INSERT INTO ai_params (params_json, score, ts, version, status, win_rate, profit_rate, trade_count) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (params_json, score, ts_val, version, status, win_rate, profit_rate, trade_count)
                )
            conn.commit()
            print(f"[保存] 保存 {len(pool)} 条 AI 参数")
        except Exception as e:
            print("[错误] 保存参数池失败:", e)
        finally:
            conn.close()

class AiSnapshotRepository:
    def __init__(self, db_path=AI_PARAMS_DB):
        self.db_path = db_path
    def _connect(self):
        return sqlite3.connect(self.db_path)
    def save(self, pool):
        try:
            conn = self._connect()
            c = conn.cursor()
            ts_iso = datetime.datetime.now().isoformat()
            snapshot_json = json.dumps(pool, ensure_ascii=False)
            c.execute(
                "INSERT INTO ai_snapshots (snapshot_time, snapshot_json) VALUES (?, ?)",
                (ts_iso, snapshot_json)
            )
            conn.commit()
            print(f"[快照] AI 参数快照已保存，时间：{ts_iso}")
        except Exception as e:
            print("[错误] 保存快照失败:", e)
        finally:
            conn.close()

_params_repo = AiParamsRepository()
_snapshot_repo = AiSnapshotRepository()

def ai_evolution():
    ai_pool = _params_repo.load_all()
    print(f"[AI进化] 当前参数池可用条数: {len(ai_pool)}")
    if not ai_pool or len(ai_pool) < 2:
        print("\033[91m[AI池为空或数量不足，跳过进化]\033[0m")
        return

    new_pool = []
    try:
        for item in ai_pool:
            if not isinstance(item, dict):
                continue
            params = merge_full_template(item.get("params", {}))
            params["TP_RATE"] = round(params["TP_RATE"] * random.uniform(0.95, 1.05), 4)
            params["SL_RATE"] = round(params["SL_RATE"] * random.uniform(0.95, 1.05), 4)
            params["MAX_POSITION_RATIO"] = min(1, max(0.01, params["MAX_POSITION_RATIO"] * random.uniform(0.9, 1.1)))
            old_version = params.get("version", "v1.0")
            major, minor = map(int, old_version.strip("v").split("."))
            minor += 1
            params["version"] = f"v{major}.{minor}"
            new_pool.append({
                "params": params,
                "score": None,
                "ts": datetime.datetime.now().isoformat(),
                "version": params["version"],
                "status": "active"
            })
        AI_POOL_MAX = 1
        new_pool = new_pool[:AI_POOL_MAX]
        _params_repo.save_all(new_pool)
        print(f"\033[92m[AI进化完毕] 共 {len(new_pool)} 组参数\033[0m")
        _snapshot_repo.save(new_pool)
    except Exception as e:
        print("[错误] AI进化失败:", e)
        traceback.print_exc()

def ai_risk_decision(signal, params=None, mode="open"):
    params = merge_full_template(params or {})
    try:
        if params["DEBUG_MODE"]:
            print(f"【AI风控】参数: {json.dumps(params, ensure_ascii=False, indent=2)}")
            print(f"【AI风控】信号: {json.dumps(signal, ensure_ascii=False, indent=2)}")
        if params["MAX_LOSS_RATIO"] > 0.2 or params["MAX_POSITION_RATIO"] > 0.5:
            return {"pass": False, "reason": "风控拒绝：风险过高"}
        if params["DEFAULT_LEVER"] > 20:
            return {"pass": False, "reason": "默认杠杆过高，风控禁止"}
        if params["SL_RATE"] > 0.05 or params["TP_RATE"] > 0.2:
            return {"pass": False, "reason": "止损或止盈参数超限"}
        if "current_trade_count" in signal and signal["current_trade_count"] >= params["MAX_TRADE_PER_DAY"]:
            return {"pass": False, "reason": "当日交易次数已满"}
        score = signal.get("score", 8)
        sl_rate = params.get("SL_RATE", 0.01)
        tp_rate = params.get("TP_RATE", 0.03)
        trailing_rate = params.get("TRAILING_STOP_RATE", 0.015)
        if score >= 9:
            trailing_rate = 0.008
            sl_rate = min(sl_rate, 0.008)
        elif score <= 6:
            trailing_rate = 0.025
            sl_rate = max(sl_rate, 0.025)
        add_pos = False
        add_pos_amount = 0.0
        vol = signal.get("vol")
        try:
            vol_f = float(vol) if vol is not None else 0.0
        except Exception:
            vol_f = 0.0
        if mode == "open" and score >= 8 and vol_f > 0:
            add_pos = True
            add_pos_amount = vol_f * 0.5
        return {
            "pass": True,
            "reason": "允许",
            "lever": params["DEFAULT_LEVER"],
            "tp": tp_rate,
            "sl": sl_rate,
            "trailing_stop": trailing_rate,
            "max_pos_ratio": params["MAX_POSITION_RATIO"],
            "stop_loss_ratio": sl_rate,
            "take_profit_ratio": tp_rate,
            "risk_interval": params["RISK_CHECK_INTERVAL"],
            "protect_margin": params["PROTECT_MARGIN_RATIO"],
            "ai_version": params["AI_VERSION"],
            "strategy_pool": params["STRATEGY_POOL"],
            "type": params["type"],
            "risk_level": params["risk_level"],
            "timeframe": params["timeframe"],
            "volatility": params["volatility"],
            "debug": params["DEBUG_MODE"],
            "add_pos": add_pos,
            "add_pos_amount": round(add_pos_amount, 6),
        }
    except Exception as e:
        print("[错误] AI风控决策异常:", e)
        traceback.print_exc()
        return {"pass": False, "reason": "风控异常"}

def multi_ai_vote(strategy):
    try:
        params = merge_full_template(strategy.get("params", {}))
        score = 7.5
        score += (0.2 - params.get("SL_RATE", 0.01)) * 10
        score += (params.get("MAX_POSITION_RATIO", 0.2) < 0.3) * 0.5
        score -= (params.get("DEFAULT_LEVER", 10) > 15) * 0.8
        score += (params.get("TP_RATE", 0.03) < 0.1) * 0.5
        score -= (params.get("FEE_RATE", 0.0005) > 0.001) * 0.5
        if params.get("risk_level") == "low":
            score += 0.4
        if params.get("type") == "trend":
            score += 0.3
        if params.get("timeframe") == "long":
            score += 0.2
        score = max(0, min(10, score + random.uniform(-0.5, 0.5)))
        return round(score, 3)
    except Exception as e:
        print("[错误] 多模型投票评分异常:", e)
        traceback.print_exc()
        return 0

def load_ai_pool(min_win_rate=0.6, min_score=6.5, top_k=5, status_filter="active"):
    ensure_ai_params_table()
    conn = sqlite3.connect(AI_PARAMS_DB)
    c = conn.cursor()
    sql = f"""SELECT params_json, score, win_rate, profit_rate, id, version
              FROM ai_params WHERE status=? AND win_rate >= ? AND score >= ?
              ORDER BY score DESC, win_rate DESC, profit_rate DESC, id ASC
              LIMIT ?"""
    c.execute(sql, (status_filter, min_win_rate, min_score, top_k))
    rows = c.fetchall()
    pool = []
    for params_json, score, win_rate, profit_rate, rid, version in rows:
        params = json.loads(params_json)
        params = merge_full_template(params)
        pool.append({
            "id": rid,
            "params": params,
            "score": score,
            "win_rate": win_rate,
            "profit_rate": profit_rate,
            "version": version
        })
    conn.close()
    return pool

__all__ = [
    "merge_full_template", "ai_evolution", "ai_risk_decision", "multi_ai_vote", "load_ai_pool"
]

def main():
    print("AI Engine (升级版) Ready. 支持参数版本管理、多模型评分、智能风控和进化闭环。")

def self_test():
    print("=== AI Engine 自检开始 ===")
    try:
        pool = _params_repo.load_all()
        print(f"[自检] 加载AI参数池条数: {len(pool)}")
        print("[自检] 执行AI进化函数测试...")
        ai_evolution()
        test_strategy = {"params": FULL_PARAM_TEMPLATE}
        score = multi_ai_vote(test_strategy)
        print(f"[自检] 多模型评分示例得分: {score}")
        test_signal = {"score": 8, "vol": 0.1, "current_trade_count": 1}
        risk = ai_risk_decision(test_signal, FULL_PARAM_TEMPLATE)
        print(f"[自检] 风控决策示例: {risk}")
    except Exception as e:
        print("[自检] 遇到错误:", e)
    print("=== AI Engine 自检完成 ===\n")

if __name__ == "__main__":
    ensure_ai_params_table()
    self_test()
    main()
