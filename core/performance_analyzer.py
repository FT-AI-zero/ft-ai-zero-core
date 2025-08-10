import os
import sqlite3
import pandas as pd
import json
import traceback
from utils.config import TRADES_DB, SIMU_TRADES_DB, STRATEGY_POOL_DB
from datetime import datetime

def load_all_trades(simulation=False):
    dbfile = SIMU_TRADES_DB if simulation else TRADES_DB
    if not os.path.exists(dbfile):
        print(f"[警告] 交易数据库文件不存在: {dbfile}")
        return pd.DataFrame()
    try:
        conn = sqlite3.connect(dbfile)
        table_name = "simulation_results" if simulation else "trades"

        # 检查表是否存在
        c = conn.cursor()
        c.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
        if not c.fetchone():
            print(f"[警告] 数据库 {dbfile} 不存在表 {table_name}，跳过加载")
            conn.close()
            return pd.DataFrame()

        df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
        conn.close()
        print(f"[加载] 成功加载 {'模拟盘' if simulation else '实盘'}交易数据，条数：{len(df)}")
    except Exception as e:
        print(f"[ERROR] 读取交易数据失败: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()

    # 去掉 meta 解析，直接补空字段
    if "strategy_id" not in df.columns:
        df["period"] = ""
        df["signal_type"] = ""
        df["strategy_id"] = df["instId"].astype(str) + "_" + df["period"] + "_" + df["signal_type"]

    return df


def load_all_strategies():
    """从策略池数据库读取所有策略记录"""
    if not os.path.exists(STRATEGY_POOL_DB):
        print(f"[警告] 策略池数据库不存在: {STRATEGY_POOL_DB}")
        return []

    try:
        conn = sqlite3.connect(STRATEGY_POOL_DB)
        c = conn.cursor()
        c.execute("SELECT strategy_name, group_name, params, score, status FROM strategy_pool")
        rows = c.fetchall()
        conn.close()
        print(f"[加载] 成功加载 策略池数据，条数：{len(rows)}")
    except Exception as e:
        print(f"[ERROR] 读取策略池失败: {e}")
        traceback.print_exc()
        return []

    strategies = []
    for strategy_name, group_name, params_data, score, status in rows:
        try:
            params = json.loads(params_data) if isinstance(params_data, str) and params_data else {}
        except Exception:
            params = {}
        strategies.append({
            'instId': strategy_name,
            'period': group_name,
            'signal': status,
            'score': score,
            'params': params,
            'last_update': None
        })
    return strategies


def analyze_strategy(trade_df, strategy, strategy_label):
    """单策略绩效分析，统计胜率、收益、最大回撤等"""
    sid = f"{strategy['instId']}_{strategy['period']}_{strategy['signal']}"
    if trade_df is None or trade_df.empty:
        print(f"[分析] 策略 {strategy_label} 无交易数据，跳过")
        return None
    df = trade_df[trade_df['strategy_id'] == sid]
    if df.empty:
        print(f"[分析] 策略 {strategy_label} 无交易记录，跳过")
        return None

    # 过滤有效成交状态
    df = df[df['status'].str.upper().isin(['FILLED', 'CLOSED', 'SUCCESS', 'WIN', 'LOSE'])]
    df = df.sort_values('ts')

    pnl = None
    for field in ['pnl', 'profit', 'pnl_amount', 'revenue']:
        if field in df.columns:
            pnl = df[field]
            break
    if pnl is None:
        print(f"[警告] 策略 {strategy_label} 无盈亏字段，跳过")
        return None

    try:
        total_pnl = pnl.sum()
        total_trades = len(df)
        win_trades = (pnl > 0).sum()
        win_rate = win_trades / total_trades if total_trades > 0 else 0
        max_profit = pnl.max()
        min_profit = pnl.min()
        max_dd = (pnl.cumsum().cummax() - pnl.cumsum()).max()
        avg_profit = pnl.mean()
        first_ts = df['ts'].min()
        last_ts = df['ts'].max()
        live_days = (last_ts - first_ts) / 86400 if first_ts and last_ts else 0
    except Exception as e:
        print(f"[ERROR] 策略分析异常 {strategy_label}: {e}")
        traceback.print_exc()
        return None

    print(f"[分析] 策略 {strategy_label} 统计完成，交易数：{total_trades}, 胜率：{win_rate:.4f}")
    return {
        'strategy_label': strategy_label,
        'strategy_id': sid,
        'total_trades': int(total_trades),
        'win_rate': round(win_rate, 4),
        'total_pnl': round(total_pnl, 4),
        'max_profit': round(max_profit, 4),
        'min_profit': round(min_profit, 4),
        'avg_profit': round(avg_profit, 4),
        'max_drawdown': round(max_dd, 4),
        'first_date': datetime.fromtimestamp(first_ts).strftime('%Y-%m-%d') if first_ts else '',
        'last_date': datetime.fromtimestamp(last_ts).strftime('%Y-%m-%d') if last_ts else '',
        'live_days': round(live_days, 2)
    }


def save_to_db(stats):
    """将策略绩效数据保存到数据库，自动创建表"""
    dbfile = TRADES_DB  # 可以改成单独的performance.db，建议分开管理
    try:
        conn = sqlite3.connect(dbfile)
        c = conn.cursor()
        c.execute("""
        CREATE TABLE IF NOT EXISTS strategy_performance (
            strategy_label TEXT,
            strategy_id TEXT,
            total_trades INTEGER,
            win_rate REAL,
            total_pnl REAL,
            max_profit REAL,
            min_profit REAL,
            avg_profit REAL,
            max_drawdown REAL,
            first_date TEXT,
            last_date TEXT,
            live_days REAL
        )
        """)
        for stat in stats:
            c.execute("""
            INSERT INTO strategy_performance (strategy_label, strategy_id, total_trades, win_rate, total_pnl, max_profit,
                min_profit, avg_profit, max_drawdown, first_date, last_date, live_days)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                stat['strategy_label'], stat['strategy_id'], stat['total_trades'], stat['win_rate'], stat['total_pnl'],
                stat['max_profit'], stat['min_profit'], stat['avg_profit'], stat['max_drawdown'], stat['first_date'],
                stat['last_date'], stat['live_days']
            ))
        conn.commit()
        conn.close()
        print(f"[保存] 策略绩效数据已保存，共 {len(stats)} 条")
    except Exception as e:
        print(f"[ERROR] 保存策略绩效失败: {e}")
        traceback.print_exc()


def main():
    print("== 策略持仓及收益统计分析器启动 ==")
    real_df = load_all_trades(simulation=False)
    simu_df = load_all_trades(simulation=True)
    strategies = load_all_strategies()

    # 临时简化匹配，清空周期和信号字段，方便与交易数据匹配
    for s in strategies:
        s['period'] = ''
        s['signal'] = ''
    print("[临时调整] 清空策略周期与信号字段，方便匹配交易数据")

    if not strategies:
        print("[警告] 策略池为空，无法统计")
        return

    stats = []
    for strat in strategies:
        label = f"{strat['instId']}_{strat['period']}_{strat['signal']}"
        st_real = analyze_strategy(real_df, strat, label)
        if st_real:
            st_real['trade_type'] = 'real'
            stats.append(st_real)
        st_simu = analyze_strategy(simu_df, strat, label)
        if st_simu:
            st_simu['trade_type'] = 'simu'
            stats.append(st_simu)

    if not stats:
        print("[警告] 未统计到任何有效策略数据")
        return

    save_to_db(stats)
    print("[完成] 策略绩效统计分析完成")

if __name__ == '__main__':
    main()
