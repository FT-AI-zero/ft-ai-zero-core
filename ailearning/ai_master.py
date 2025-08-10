import os
import json
import datetime
import random
import sqlite3
import traceback

from utils.config import AI_PARAMS_DB
from ailearning.ai_engine import (
    merge_full_template,
    ai_evolution,
    multi_ai_vote,
    ai_risk_decision,
    AiParamsRepository,
    AiSnapshotRepository,
)

AI_PARAMS_REQUIRED_FIELDS = {
    "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
    "params_json": "TEXT",
    "score": "REAL",
    "ts": "TEXT",
    "version": "TEXT",
    "status": "TEXT",
    "win_rate": "REAL",
    "profit_rate": "REAL",
    "trade_count": "INTEGER"
}
AI_SNAPSHOT_REQUIRED_FIELDS = {
    "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
    "params_snapshot": "TEXT",
    "ts": "TEXT"
}

# 表结构升级自愈
def ensure_ai_params_table():
    conn = sqlite3.connect(AI_PARAMS_DB)
    c = conn.cursor()
    # 创建表结构
    c.execute("""
        CREATE TABLE IF NOT EXISTS ai_params (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            params_json TEXT,
            score REAL,
            ts TEXT,
            version TEXT,
            status TEXT,
            win_rate REAL DEFAULT 0,
            profit_rate REAL DEFAULT 0,
            trade_count INTEGER DEFAULT 0
        )
    """)
    # 补字段
    fields = [r[1] for r in c.execute("PRAGMA table_info(ai_params)")]
    for k, v in AI_PARAMS_REQUIRED_FIELDS.items():
        if k not in fields:
            print(f"[升级] 自动补全 ai_params 字段: {k}")
            c.execute(f"ALTER TABLE ai_params ADD COLUMN {k} {v}")
    conn.commit()
    # 快照表
    c.execute("""
        CREATE TABLE IF NOT EXISTS ai_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            params_snapshot TEXT,
            ts TEXT
        )
    """)
    conn.commit()
    conn.close()
    print("[建表] ai_params 和 ai_snapshots 表结构已完成自愈")

# 批量修正所有active状态
def fix_all_status():
    print("【批量自愈】同步参数池 status/active 字段")
    conn = sqlite3.connect(AI_PARAMS_DB)
    c = conn.cursor()
    count = 0
    for rid, params_json in c.execute("SELECT id, params_json FROM ai_params"):
        try:
            params = json.loads(params_json)
            params["status"] = "active"
            if "win_rate" not in params or params["win_rate"] is None:
                params["win_rate"] = 1.0
            c.execute(
                "UPDATE ai_params SET params_json=?, status='active', win_rate=? WHERE id=?",
                (json.dumps(params, ensure_ascii=False), params["win_rate"], rid)
            )
            count += 1
        except Exception as e:
            print(f"[修复异常] id={rid} {e}")
    conn.commit()
    conn.close()
    print(f"【批量自愈】已同步修正 {count} 条参数 status=active，win_rate=1.0")

# 自动补种
def ensure_ai_params_seed(min_count=50):
    repo = AiParamsRepository()
    pool = repo.load_all(status_filter="active")
    if len(pool) < min_count:
        print("【AI参数池】数量不足，自动补种子参数！")
        need = min_count - len(pool)
        to_save = []
        for _ in range(need):
            params = merge_full_template({})
            params["status"] = "active"
            params["TP_RATE"] = round(params["TP_RATE"] * (0.95 + 0.1 * random.random()), 4)
            params["SL_RATE"] = round(params["SL_RATE"] * (0.95 + 0.1 * random.random()), 4)
            to_save.append({
                "params": params,
                "score": 7.0,
                "ts": datetime.datetime.now().isoformat(),
                "version": params["version"],
                "status": "active"
            })
        conn = sqlite3.connect(AI_PARAMS_DB)
        c = conn.cursor()
        for item in to_save:
            c.execute(
                "INSERT INTO ai_params (params_json, score, ts, version, status) VALUES (?, ?, ?, ?, ?)",
                (json.dumps(item["params"], ensure_ascii=False), item["score"], item["ts"], item["version"], item["status"])
            )
        conn.commit()
        conn.close()
        print(f"【AI参数池】已补充 {need} 条种子参数，总数达到 {len(pool) + need} 条")
    else:
        print(f"【AI参数池】数量充足，当前共 {len(pool)} 条。")

# 修复所有参数字段补全
def repair_all_params():
    print("【参数池批量修复/补全】")
    conn = sqlite3.connect(AI_PARAMS_DB)
    c = conn.cursor()
    rows = c.execute("SELECT id, params_json FROM ai_params").fetchall()
    for rid, params_json in rows:
        try:
            params = merge_full_template(json.loads(params_json))
            params["status"] = "active"
            c.execute(
                "UPDATE ai_params SET params_json=?, status=? WHERE id=?",
                (json.dumps(params, ensure_ascii=False), "active", rid)
            )
        except Exception as e:
            print(f"[参数池修复异常] id={rid} {e}")
    conn.commit()
    print(f"[参数池修复] 完成，共 {len(rows)} 条")
    conn.close()

# 更新胜率和状态
def update_parameter_performance(WIN_RATE_THRESHOLD=0.6):
    print("[复盘更新] 开始更新参数胜率及状态")
    conn = sqlite3.connect(AI_PARAMS_DB)
    c = conn.cursor()
    data = c.execute("SELECT id, win_rate FROM ai_params").fetchall()
    updated = 0
    for pid, win_rate in data:
        new_status = "active" if (win_rate or 0) >= WIN_RATE_THRESHOLD else "inactive"
        row = c.execute("SELECT params_json FROM ai_params WHERE id=?", (pid,)).fetchone()
        if row:
            params = json.loads(row[0])
            params["status"] = new_status
            c.execute(
                "UPDATE ai_params SET status=?, win_rate=?, params_json=? WHERE id=?",
                (new_status, win_rate, json.dumps(params, ensure_ascii=False), pid)
            )
            updated += 1
    conn.commit()
    print(f"[复盘更新] 完成 {updated} 条参数状态更新")
    conn.close()

# AI进化
def do_ai_evolution():
    print("【AI进化流程】")
    ai_evolution()
    print("[AI进化] 执行成功")

# 评分+快照
def archive_and_score_ai_pool():
    print("【AI参数池评分&快照归档】")
    repo = AiParamsRepository()
    snapshot_repo = AiSnapshotRepository()
    pool = repo.load_all(status_filter="active")
    conn = sqlite3.connect(AI_PARAMS_DB)
    c = conn.cursor()
    for item in pool:
        data = item["params"]
        data["status"] = "active"
        score = multi_ai_vote({"params": data})
        data["score"] = score
        c.execute(
            "UPDATE ai_params SET params_json=?, score=?, status=? WHERE id=?",
            (json.dumps(data, ensure_ascii=False), score, "active", item["id"])
        )
    conn.commit()
    snapshot_repo.save(pool)
    print(f"[快照归档] 当前AI参数池快照已存入数据库，时间：{datetime.datetime.now().isoformat()}")
    conn.close()

# 风控打分
def ai_risk_scoring_all():
    print("【AI风控批量打分】")
    repo = AiParamsRepository()
    pool = repo.load_all(status_filter="active")
    conn = sqlite3.connect(AI_PARAMS_DB)
    c = conn.cursor()
    for item in pool:
        data = item["params"]
        data["status"] = "active"
        risk_info = ai_risk_decision({}, params=data)
        data["risk_score"] = risk_info.get("risk_score", 0.5)
        data["ai_pass"] = risk_info.get("pass", True)
        data["ai_reason"] = risk_info.get("reason", "")
        c.execute(
            "UPDATE ai_params SET params_json=?, status=? WHERE id=?",
            (json.dumps(data, ensure_ascii=False), "active", item["id"])
        )
    conn.commit()
    print(f"[AI风控打分] 完成 {len(pool)} 条")
    conn.close()

# 优胜劣汰，仅status切换不删除
def rotate_ai_params(top_k=10, min_win_rate=0.6, min_score=6.5):
    print("[AI参数池轮换] 执行优胜劣汰")
    repo = AiParamsRepository()
    pool = repo.load_all(status_filter="active")
    if not pool:
        print("[AI参数池轮换] 无可用参数，跳过")
        return
    pool = sorted(pool, key=lambda x: (x.get("win_rate",0), x.get("score",0)), reverse=True)
    winners = pool[:top_k]
    winner_ids = set([item['id'] for item in winners])
    conn = sqlite3.connect(AI_PARAMS_DB)
    c = conn.cursor()
    for item in pool:
        status = "active" if item["id"] in winner_ids else "inactive"
        params = item["params"]
        params["status"] = status
        c.execute(
            "UPDATE ai_params SET status=?, params_json=? WHERE id=?",
            (status, json.dumps(params, ensure_ascii=False), item["id"])
        )
    conn.commit()
    print(f"[AI参数池轮换] 保留前{top_k}组参数 active，其余设为 inactive")
    conn.close()

def main():
    print("==== AI 主控调度唯一池全流程(DB版) ====")
    ensure_ai_params_table()
    fix_all_status()
    ensure_ai_params_seed(min_count=50)
    repair_all_params()
    update_parameter_performance(WIN_RATE_THRESHOLD=0.6)
    do_ai_evolution()
    archive_and_score_ai_pool()
    ai_risk_scoring_all()
    rotate_ai_params(top_k=10, min_win_rate=0.6, min_score=6.5)
    print("==== AI 主控唯一池闭环已完成！（DB版） ====")

if __name__ == "__main__":
    main()
