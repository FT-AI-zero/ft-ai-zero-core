from utils.db_upgrade import ensure_table_fields
from utils.config import STRATEGY_POOL_DB, NOSTRATEGY_POOL_DB, DB_DIR
from ailearning.ai_engine import multi_ai_vote, ai_evolution, load_ai_pool
import os
import sqlite3
import datetime
import json
import random
from collections import defaultdict
import pandas as pd

FEATURE_PERIODS = ["1m", "3m", "5m", "15m", "1H", "4H", "1D"]

STRATEGY_REQUIRED_FIELDS = {
    "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
    "symbol": "TEXT",
    "group_name": "TEXT",
    "params": "TEXT",
    "score": "REAL",
    "last_eval_time": "TEXT",
    "pool_layer": "TEXT",
    "update_time": "TEXT"
}

def get_all_strategies(db_path):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute("SELECT id, symbol, group_name, params, score FROM strategies")
    rows = c.fetchall()
    conn.close()
    strategies = []
    for _id, symbol, group, params_json, score in rows:
        strategies.append({
            "id": _id,
            "symbol": symbol,
            "group": group or "default",
            "params": params_json,
            "score": score or 0,
        })
    return strategies

def update_strategy_score(db_path, id_score_list):
    ts = datetime.datetime.now().isoformat()
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    for _id, score in id_score_list:
        c.execute(
            "UPDATE strategies SET score=?, last_eval_time=? WHERE id=?",
            (score, ts, _id)
        )
    conn.commit()
    conn.close()

def move_to_nostrategy(ids):
    if not ids:
        return
    src = sqlite3.connect(STRATEGY_POOL_DB)
    dst = sqlite3.connect(NOSTRATEGY_POOL_DB)
    cs = src.cursor()
    cd = dst.cursor()
    for _id in ids:
        cs.execute(
            "SELECT symbol, group_name, params, score FROM strategies WHERE id=?", (_id,)
        )
        row = cs.fetchone()
        if not row:
            continue
        symbol, group, params_json, score = row
        cd.execute(
            "INSERT INTO strategies(symbol, group_name, params, score, last_eval_time) VALUES(?,?,?,?,?)",
            (symbol, group, params_json, score, datetime.datetime.now().isoformat())
        )
        cs.execute("DELETE FROM strategies WHERE id=?", (_id,))
    src.commit()
    dst.commit()
    src.close()
    dst.close()

def load_features_for_symbol(symbol):
    combined_features = {}
    conn = None
    try:
        conn = sqlite3.connect(os.path.join(DB_DIR, "features.db"))
        for period in FEATURE_PERIODS:
            table = f"features_{period}"
            sql = f"SELECT * FROM {table} WHERE instId=? ORDER BY ts DESC LIMIT 1"
            df = pd.read_sql_query(sql, conn, params=(symbol,))
            if df.empty:
                print(f"[警告] 缺失 {period} 周期指标数据 for {symbol}")
                return None
            row = df.iloc[0].to_dict()
            for k in ["instId", "ts"]:
                if k in row:
                    del row[k]
            prefixed = {f"{period}_{key}": val for key, val in row.items()}
            combined_features.update(prefixed)
    except Exception as e:
        print(f"[异常] 读取指标数据失败: {e}")
        return None
    finally:
        if conn:
            conn.close()
    return combined_features

def archive_strategy_pool(keep_ids, remove_ids, scores_dict):
    try:
        review_path = os.path.join(DB_DIR, "strategy_pool_review.log")
        with open(review_path, "a", encoding="utf-8") as f:
            f.write(json.dumps({
                "ts": datetime.datetime.now().isoformat(),
                "keep_ids": keep_ids,
                "remove_ids": remove_ids,
                "scores": scores_dict
            }, ensure_ascii=False) + "\n")
        print(f"[归档] 已写入策略池晋级/淘汰快照 {review_path}")
    except Exception as e:
        print(f"[归档异常] 策略池晋级/淘汰写入失败: {e}")

def get_real_symbols_from_kline(period="1m", max_count=100):
    conn = sqlite3.connect(os.path.join(DB_DIR, f'kline_{period}.db'))
    c = conn.cursor()
    try:
        c.execute("SELECT DISTINCT instId FROM kline_{} LIMIT ?".format(period), (max_count,))
        symbols = [row[0] for row in c.fetchall()]
        return symbols
    except Exception as e:
        print(f"[提取币种异常] {e}")
        return []
    finally:
        conn.close()

def ensure_strategy_pool_seed(min_count=20):
    conn = sqlite3.connect(STRATEGY_POOL_DB)
    c = conn.cursor()
    c.execute("SELECT symbol FROM strategies")
    existing_symbols = set([row[0] for row in c.fetchall()])
    cnt = len(existing_symbols)
    if cnt < min_count:
        all_real_symbols = get_real_symbols_from_kline("1m", max_count=200)
        # **这里加一层白名单过滤，只允许标准OKX合约/现货格式，比如 "BTC-USDT-SWAP" 或 "ETH-USDT"**
        def is_real_symbol(symbol):
            # 支持 "BTC-USDT-SWAP", "ETH-USDT", "ETH-USD-SWAP" 等
            if not isinstance(symbol, str):
                return False
            if any(symbol.startswith(prefix) for prefix in ["EXAMPLE", "default_", "RANDOM_", "auto_"]):
                return False
            # 正常币种格式判定（最基础）
            if ("USDT" in symbol or "USD" in symbol) and "-" in symbol:
                return True
            return False

        all_real_symbols = [sym for sym in all_real_symbols if is_real_symbol(sym)]
        to_pick = [sym for sym in all_real_symbols if sym not in existing_symbols]
        to_add = []
        for i, symbol in enumerate(to_pick):
            if len(to_add) + cnt >= min_count:
                break
            group_name = random.choice(["trend", "mean_revert", "momentum"])
            params = json.dumps({
                "TP_RATE": round(random.uniform(0.01, 0.06), 4),
                "SL_RATE": round(random.uniform(0.008, 0.04), 4),
                "MAX_POSITION_RATIO": round(random.uniform(0.08, 0.25), 4),
                "strategy_id": f"auto_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}_{i}",
                "version": "v1.0"
            }, ensure_ascii=False)
            to_add.append((symbol, group_name, params, 0, datetime.datetime.now().isoformat()))
        if to_add:
            c.executemany(
                "INSERT INTO strategies (symbol, group_name, params, score, last_eval_time) VALUES (?, ?, ?, ?, ?)",
                to_add
            )
            conn.commit()
            print(f"[策略池补种] 补充{len(to_add)}条真实币种策略")
        else:
            print("[策略池补种] 没有新币种可补充，或已达上限")
    else:
        print(f"[策略池] 当前策略币种数：{cnt}，无需补种")
    conn.close()


def active_pool_manager():
    print("==== 策略池全闭环主控(DB)启动 ====")
    # ——自动补字段！——
    ensure_table_fields(STRATEGY_POOL_DB, "strategies", STRATEGY_REQUIRED_FIELDS)
    ensure_table_fields(NOSTRATEGY_POOL_DB, "strategies", STRATEGY_REQUIRED_FIELDS)
    # 用真实币种自动补种
    ensure_strategy_pool_seed(min_count=20)

    strategies = get_all_strategies(STRATEGY_POOL_DB)
    if not strategies:
        print("[策略池] 未发现策略，退出")
        return

    ai_param_groups = load_ai_pool(min_win_rate=0, min_score=0, top_k=3)
    if not ai_param_groups:
        ai_param_groups = [{"id": 0, "params": {}}]

    scores = []
    scores_dict = {}

    for s in strategies:
        try:
            s_params = json.loads(s["params"])
        except Exception:
            s_params = {}

        required_periods = s_params.get("required_periods", FEATURE_PERIODS)
        features = load_features_for_symbol(s["symbol"])
        if features is None:
            print(f"[跳过] {s['symbol']} 缺失多周期指标，评分跳过")
            continue

        best_score = 0
        best_param_id = None
        group_scores = []
        for group in ai_param_groups:
            try:
                score = multi_ai_vote({"params": s_params, "features": features, "ai_param": group["params"]})
                group_scores.append((group.get("id", 0), score))
                if score > best_score:
                    best_score = score
                    best_param_id = group.get("id", 0)
            except Exception as e:
                print(f"[评分异常] 策略{str(s['id'])}-{s['symbol']} Param{group.get('id',0)} : {e}")
                continue
        scores.append((s['id'], best_score))
        scores_dict[s['id']] = {
            "symbol": s["symbol"],
            "best_score": best_score,
            "best_param_id": best_param_id,
            "group_scores": group_scores
        }

    update_strategy_score(STRATEGY_POOL_DB, scores)

    groups = defaultdict(list)
    for s in strategies:
        groups[s['group']].append(s)

    TOP_N = 3
    keep_ids = []
    remove_ids = []
    for grp, pool in groups.items():
        pool.sort(key=lambda x: x['score'], reverse=True)
        keep_ids.extend([s['id'] for s in pool[:TOP_N]])
        remove_ids.extend([s['id'] for s in pool[TOP_N:]])

    move_to_nostrategy(remove_ids)
    archive_strategy_pool(keep_ids, remove_ids, scores_dict)

    try:
        ai_evolution()
    except Exception as e:
        print(f"[错误] AI进化失败: {e}")

    print(f"[完成] 活跃策略 {len(keep_ids)} 条，淘汰策略 {len(remove_ids)} 条。")

if __name__ == '__main__':
    active_pool_manager()
