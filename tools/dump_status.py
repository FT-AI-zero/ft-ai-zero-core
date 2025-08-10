# tools/dump_status.py
import os, time, sqlite3, json, datetime as dt
from utils.config import SIGNAL_POOL_DB, STRATEGY_POOL_DB, REVIEW_DB, AI_PARAMS_DB, LOG_DIR

def tail(path, n=20):
    try:
        with open(path, 'r', encoding='utf-8', errors='ignore') as f:
            return ''.join(f.readlines()[-n:])
    except Exception as e:
        return f'(no file: {path})'

def mtime(path):
    try:
        return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(os.path.getmtime(path)))
    except: return 'N/A'

def main():
    print('=== STATUS SNAPSHOT ===')
    print('cwd =', os.path.abspath(os.getcwd()))
    print('time =', dt.datetime.now())
    print('--- DB paths ---')
    print('signals  =', SIGNAL_POOL_DB)
    print('ai_params=', AI_PARAMS_DB)
    print('review   =', REVIEW_DB)
    print('spool    =', STRATEGY_POOL_DB)

    # signals 统计
    con = sqlite3.connect(SIGNAL_POOL_DB)
    counts = dict(con.execute("select status, count(1) from signals group by status").fetchall() or [])
    latest3 = con.execute("select id,instId,coalesce(period,interval),signal_type,status,substr(meta,1,60) from signals order by id desc limit 3").fetchall()
    con.close()
    print('\n--- signals counts ---')
    print(counts)
    print('latest3:', latest3)

    # allowlist Top10
    con = sqlite3.connect(STRATEGY_POOL_DB)
    allow_top = con.execute("select param_group_id, window, score, trades, source, created_at from allowlist order by score desc, trades desc limit 10").fetchall()
    con.close()
    print('\n--- allowlist top10 ---')
    for r in allow_top: print(' ', r)

    # live_trades 最新 5
    con = sqlite3.connect(REVIEW_DB)
    rows = con.execute("select id,instId,side,price,vol,gid,ts from live_trades order by id desc limit 5").fetchall()
    con.close()
    print('\n--- live_trades latest5 ---')
    for r in rows: print(' ', r)

    # 关键日志最后 20 行
    logs = [
        'trade_engine.log',
        'zero_engine.log',
        'position_guard.log',
        'signal_gen.log',
        'live_dist.log',
        'scheduler.log',
    ]
    print('\n--- tail logs (last 20 lines) ---')
    for name in logs:
        path = os.path.join(LOG_DIR, name)
        print(f'\n[{name}] mtime={mtime(path)}')
        print(tail(path, 20))

if __name__ == '__main__':
    main()
