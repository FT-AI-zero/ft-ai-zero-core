import os
import sqlite3
import json
import datetime
import traceback
from ailearning.ai_engine import ai_risk_decision, ai_evolution, merge_full_template, load_ai_pool
from utils.config import TRADES_DB, REVIEW_DB, SIMU_TRADES_DB
from utils.db_upgrade import ensure_table_fields

# ==== 表字段模板 ====
REVIEW_REQUIRED_FIELDS = {
    "review": {
        "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
        "review_time": "TEXT",
        "summary": "TEXT",
        "details": "TEXT"
    },
    "superloss": {
        "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
        "ts": "TEXT",
        "trade": "TEXT"
    },
    "group_stats": {
        "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
        "group_id": "INTEGER",
        "score": "REAL",
        "win_rate": "REAL",
        "profit": "REAL",
        "total_trades": "INTEGER",
        "max_drawdown": "REAL",
        "review_time": "TEXT",
        "summary_json": "TEXT"
    }
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

def ensure_review_tables():
    conn = sqlite3.connect(REVIEW_DB)
    for table, fields in REVIEW_REQUIRED_FIELDS.items():
        cur = conn.cursor()
        cur.execute(
            f"""CREATE TABLE IF NOT EXISTS {table} (
            {', '.join([f"{k} {v}" for k, v in fields.items()])}
            )""")
        ensure_table_fields(REVIEW_DB, table, fields)
    conn.commit()
    conn.close()

def fetch_all_trades(db_path, limit=100000):
    ensure_table_fields(db_path, "trades", TRADES_REQUIRED_FIELDS)
    try:
        conn = sqlite3.connect(db_path)
        c = conn.cursor()
        c.execute("SELECT * FROM trades ORDER BY ts DESC LIMIT ?", (limit,))
        rows = c.fetchall()
        fields = [col[0] for col in c.description]
        trades = [dict(zip(fields, row)) for row in rows]
        conn.close()
        return trades
    except Exception as e:
        print(f"[ERROR] 读取 trades 失败: {e}")
        return []

def save_review_to_db(summary, details):
    try:
        conn = sqlite3.connect(REVIEW_DB)
        c = conn.cursor()
        c.execute(
            "INSERT INTO review (review_time, summary, details) VALUES (?, ?, ?)",
            (
                datetime.datetime.now().isoformat(),
                json.dumps(summary, ensure_ascii=False),
                json.dumps(details, ensure_ascii=False)
            )
        )
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[ERROR] 存储 review 失败: {e}")

def save_group_stats_to_db(stats_list):
    if not stats_list:
        return
    try:
        conn = sqlite3.connect(REVIEW_DB)
        c = conn.cursor()
        for stat in stats_list:
            c.execute("""
                INSERT INTO group_stats (group_id, score, win_rate, profit, total_trades, max_drawdown, review_time, summary_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                stat.get('group_id'), stat.get('score'), stat.get('win_rate'), stat.get('profit'),
                stat.get('total_trades'), stat.get('max_drawdown'),
                datetime.datetime.now().isoformat(),
                json.dumps(stat, ensure_ascii=False)
            ))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[ERROR] 存储 group_stats 失败: {e}")

def save_superloss_to_db(trades):
    if not trades:
        return
    try:
        conn = sqlite3.connect(REVIEW_DB)
        c = conn.cursor()
        for t in trades:
            c.execute(
                "INSERT INTO superloss (ts, trade) VALUES (?, ?)",
                (
                    datetime.datetime.now().isoformat(),
                    json.dumps(t, ensure_ascii=False)
                )
            )
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[ERROR] 存储 superloss 失败: {e}")

def calc_max_drawdown(pnl_list):
    peak = trough = pnl = 0
    max_dd = 0
    for v in pnl_list:
        pnl += v
        if pnl > peak:
            peak = pnl
            trough = pnl
        if pnl < trough:
            trough = pnl
        max_dd = min(max_dd, trough - peak)
    return abs(max_dd)

# 主复盘逻辑（多AI参数组自动评分/归档/进化）——新版支持 db_path/source_label
def review_trades(db_path, source_label="simu"):
    ensure_review_tables()
    trades = fetch_all_trades(db_path=db_path, limit=100000)
    if not trades:
        print(f"[复盘][{source_label}] 无历史交易，退出")
        return

    ai_param_groups = load_ai_pool(min_win_rate=0, min_score=0, top_k=10)
    if not ai_param_groups:
        print(f"[复盘][{source_label}] 没有AI参数组，采用默认参数组归档")
        ai_param_groups = [{"id": 0, "params": merge_full_template({}), "score": 0}]

    all_group_stats = []
    superloss_records = []

    print(f"[复盘][{source_label}] 本次共 {len(ai_param_groups)} 组AI参数归档评分...")

    for group in ai_param_groups:
        group_id = group.get("id", 0)
        params = group.get("params", {})
        score = group.get("score", 0)
        open_map = {}
        details = []
        pnl_list = []
        num_win = num_loss = total_win = total_loss = 0
        max_profit = None
        max_loss = None

        for tr in trades:
            ai_params = params
            tr['ai_params'] = ai_params
            res = ai_risk_decision(tr, params=ai_params, mode='all')
            tr['ai_score'] = res.get('risk_score', 0.5)
            tr['ai_pass'] = res.get('pass', True)
            tr['ai_reason'] = res.get('reason', '')
            comment = (tr.get('comment') or '').lower()
            # --------- 防止None和脏数据 ----------
                        # --------- 防止None和脏数据 ----------
            vol_raw = tr.get('vol')
            try:
                vol = abs(float(vol_raw)) if vol_raw not in (None, "", "None") else 0
            except Exception:
                vol = 0

            price_raw = tr.get('price')
            try:
                price = float(price_raw) if price_raw not in (None, "", "None") else 0
            except Exception:
                price = 0
            # --------------------------------------

            # --------------------------------------
            ts = tr.get('ts')
            instId = tr.get('instId')
            sid = tr.get('strategy_id')
            key = (instId, sid, vol)
            if 'open' in comment:
                side = 'buy' if 'buy' in comment else 'sell' if 'sell' in comment else 'unknown'
                open_map[key] = (price, side, ts)
            elif 'close' in comment and key in open_map:
                open_price, side, open_ts = open_map.pop(key)
                pnl = (price - open_price) * vol if side == 'buy' else (open_price - price) * vol
                pnl_list.append(pnl)
                tr['pnl'] = pnl
                is_win = pnl >= 0
                if is_win:
                    num_win += 1
                    total_win += pnl
                else:
                    num_loss += 1
                    total_loss += pnl
                    if pnl < -0.1:
                        superloss_records.append(tr)
                if max_profit is None or pnl > max_profit:
                    max_profit = pnl
                if max_loss is None or pnl < max_loss:
                    max_loss = pnl
                details.append({
                    'instId': instId,
                    'vol': vol,
                    'open_price': open_price,
                    'close_price': price,
                    'pnl': pnl,
                    'open_ts': open_ts,
                    'close_ts': ts,
                    'result': 'win' if is_win else 'loss',
                    'param_group_id': group_id,
                    'source': source_label
                })

        total_trades = num_win + num_loss
        win_rate = (num_win / total_trades * 100) if total_trades else 0
        max_drawdown = calc_max_drawdown(pnl_list)
        summary = {
            'param_group_id': group_id,
            'score': score,
            'total_trades': total_trades,
            'win_rate': round(win_rate, 2),
            'total_win': round(total_win, 4),
            'total_loss': round(total_loss, 4),
            'max_profit': round(max_profit, 4) if max_profit is not None else None,
            'max_loss': round(max_loss, 4) if max_loss is not None else None,
            'max_drawdown': round(max_drawdown, 4),
            'timestamp': datetime.datetime.now().isoformat(),
            'source': source_label
        }
        all_group_stats.append(summary)
        print(f"\n==== 复盘[{source_label}]-参数组ID={group_id} ====")
        print(f"总交易: {summary['total_trades']} | 胜率: {summary['win_rate']:.2f}% | 累计盈亏: {summary['total_win']+summary['total_loss']:.2f}")
        print(f"最大盈利: {summary['max_profit']} | 最大亏损: {summary['max_loss']} | 最大回撤: {summary['max_drawdown']:.2f}")
        save_review_to_db(summary, details)

    save_superloss_to_db(superloss_records)
    save_group_stats_to_db(all_group_stats)
    update_ai_params_winrate_from_review(all_group_stats)
    print(f"[复盘归档][{source_label}] 所有参数组结果已写入 review.db")
    print(f"[AI进化][{source_label}] 完成一次复盘演化。")
    try:
        ai_evolution()
    except Exception as e:
        print(f"[AI进化][{source_label}] 调用异常:", e)
        traceback.print_exc()

def update_ai_params_winrate_from_review(stats_list):
    import sqlite3
    from utils.config import AI_PARAMS_DB
    conn = sqlite3.connect(AI_PARAMS_DB)
    c = conn.cursor()
    updated = 0
    for stat in stats_list:
        group_id = stat.get("param_group_id")
        win_rate = stat.get("win_rate")
        score = stat.get("score")
        if group_id is not None and win_rate is not None and score is not None:
            c.execute("UPDATE ai_params SET win_rate=?, score=? WHERE id=?",
                      (win_rate, score, group_id))
            updated += 1
    conn.commit()
    conn.close()
    print(f"[AI参数池同步] 已写回 {updated} 条 win_rate/score")

if __name__ == '__main__':
    review_trades(db_path=SIMU_TRADES_DB, source_label="simu")
    review_trades(db_path=TRADES_DB, source_label="real")