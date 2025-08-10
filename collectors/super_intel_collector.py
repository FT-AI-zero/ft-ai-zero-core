import os
import time
import datetime
import sqlite3
import requests
import traceback
from core.okx_trader import OKXTrader
from utils.config import DB_DIR
from utils.db_upgrade import ensure_table_fields

FAILED_COLOR = '\033[91m'
OK_COLOR = '\033[92m'
RESET_COLOR = '\033[0m'

def ensure_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)
ensure_dir(DB_DIR)

def ensure_tables():
    tables = [
        ("leaderboard.db", "leaderboard", {
            "user": "TEXT", "instId": "TEXT", "pnl": "REAL", "ts": "INTEGER"
        }, '''
            CREATE TABLE IF NOT EXISTS leaderboard (
                user TEXT, instId TEXT, pnl REAL, ts INTEGER,
                PRIMARY KEY(user, instId, ts)
            )'''),
        ("news.db", "news", {
            "id": "TEXT", "title": "TEXT", "url": "TEXT", "ts": "INTEGER"
        }, '''
            CREATE TABLE IF NOT EXISTS news (
                id TEXT PRIMARY KEY, title TEXT, url TEXT, ts INTEGER
            )'''),
        ("whale_trades.db", "whale_trades", {
            "instId": "TEXT", "ts": "INTEGER", "size": "REAL", "side": "TEXT"
        }, '''
            CREATE TABLE IF NOT EXISTS whale_trades (
                instId TEXT, ts INTEGER, size REAL, side TEXT,
                PRIMARY KEY(instId, ts, side)
            )'''),
        ("hot_search.db", "hot_search", {
            "keyword": "TEXT", "ts": "INTEGER"
        }, '''
            CREATE TABLE IF NOT EXISTS hot_search (
                keyword TEXT, ts INTEGER,
                PRIMARY KEY(keyword, ts)
            )''')
    ]
    for dbfile, tablename, fields, schema in tables:
        conn = sqlite3.connect(os.path.join(DB_DIR, dbfile))
        c = conn.cursor()
        c.execute(schema)
        ensure_table_fields(os.path.join(DB_DIR, dbfile), tablename, fields)
        conn.commit()
        conn.close()

def save_to_db(dbfile, tablename, fields, values):
    full_path = os.path.join(DB_DIR, dbfile)
    conn = sqlite3.connect(full_path)
    c = conn.cursor()
    q = f'INSERT OR IGNORE INTO {tablename} ({",".join(fields)}) VALUES ({",".join(["?"]*len(fields))})'
    try:
        c.execute(q, values)
        conn.commit()
    except Exception as e:
        print(f"{FAILED_COLOR}[ERROR] 写入 {tablename} 失败: {e}{RESET_COLOR}")
    conn.close()

def clean_expired(dbfile, tablename, time_field="ts", days=15):
    full_path = os.path.join(DB_DIR, dbfile)
    conn = sqlite3.connect(full_path)
    c = conn.cursor()
    expire_ts = int(time.time()) - days*24*3600
    try:
        c.execute(f"DELETE FROM {tablename} WHERE {time_field} < ?", (expire_ts,))
        conn.commit()
        print(f"{OK_COLOR}[CLEAN] {tablename}: 已清理{days}天前数据{RESET_COLOR}")
    except Exception as e:
        print(f"{FAILED_COLOR}[CLEAN ERROR] {tablename}: {e}{RESET_COLOR}")
    conn.close()

def fetch_and_save_leaderboard(trader):
    try:
        data = trader.get_leaderboard()
        now = int(time.time())
        for item in data:
            user = item.get("user")
            instId = item.get("instId")
            pnl = item.get("pnl", 0)
            save_to_db("leaderboard.db", "leaderboard", ["user", "instId", "pnl", "ts"], [user, instId, pnl, now])
        print(f"{OK_COLOR}[牛人榜] 已采集{len(data)}条{RESET_COLOR}")
    except Exception as e:
        print(f"{FAILED_COLOR}[牛人榜采集异常]: {e}{RESET_COLOR}")

def fetch_and_save_news():
    try:
        url = "https://newsapi.org/v2/top-headlines?category=business&language=zh"
        r = requests.get(url, timeout=10)
        data = r.json()
        now = int(time.time())
        for article in data.get("articles", []):
            title = article.get("title", "")
            news_id = article.get("url", "")
            url_link = article.get("url", "")
            save_to_db("news.db", "news", ["id", "title", "url", "ts"], [news_id, title, url_link, now])
        print(f"{OK_COLOR}[新闻] 已采集{len(data.get('articles', []))}条{RESET_COLOR}")
    except Exception as e:
        print(f"{FAILED_COLOR}[新闻采集异常]: {e}{RESET_COLOR}")

def fetch_and_save_whale_trades(trader, instId):
    # 需在 okx_trader.py 实现 get_whale_trades()
    try:
        data = trader.get_whale_trades(instId)
        for item in data:
            ts = int(item["ts"])
            size = float(item["size"])
            side = item["side"]
            save_to_db("whale_trades.db", "whale_trades", ["instId", "ts", "size", "side"], [instId, ts, size, side])
        print(f"{OK_COLOR}[鲸鱼交易] {instId} 采集{len(data)}条{RESET_COLOR}")
    except Exception as e:
        print(f"{FAILED_COLOR}[鲸鱼采集异常]: {e}{RESET_COLOR}")

def fetch_and_save_hot_search():
    try:
        url = "https://trends.baidu.com/trendsearch"
        r = requests.get(url, timeout=10)
        now = int(time.time())
        # 这里只做演示，真实请解析html或json
        for kw in ["BTC", "ETH", "DOGE"]:
            save_to_db("hot_search.db", "hot_search", ["keyword", "ts"], [kw, now])
        print(f"{OK_COLOR}[热搜] 已采集关键词{RESET_COLOR}")
    except Exception as e:
        print(f"{FAILED_COLOR}[热搜采集异常]: {e}{RESET_COLOR}")

def main():
    print("===== 智能情报超级采集器启动 =====")
    ensure_dir(DB_DIR)
    ensure_tables()
    trader = OKXTrader()

    insts = trader.get_all_instruments("SWAP")
    inst_ids = [x['instId'] for x in insts]  # <=== 全币种，不再用focus_symbols

    while True:
        fetch_and_save_leaderboard(trader)
        fetch_and_save_news()
        for instId in inst_ids:
            fetch_and_save_whale_trades(trader, instId)
            time.sleep(0.05)
        fetch_and_save_hot_search()
        # 数据清理
        clean_expired("leaderboard.db", "leaderboard", days=15)
        clean_expired("news.db", "news", days=10)
        clean_expired("whale_trades.db", "whale_trades", days=10)
        clean_expired("hot_search.db", "hot_search", days=7)
        print(f"{OK_COLOR}本轮情报采集完成{RESET_COLOR}")
        time.sleep(90)


def load_focus_symbols():
    config_file = os.path.join(DB_DIR, "focus_symbols.json")
    if not os.path.exists(config_file):
        with open(config_file, "w") as f:
            import json
            json.dump([
                "BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP","TON-USDT-SWAP",
                "DOGE-USDT-SWAP","XRP-USDT-SWAP","PIU-USDT-SWAP","PUMP-USDT-SWAP"
            ], f)
    with open(config_file, "r") as f:
        import json
        return json.load(f)

if __name__ == "__main__":
    main()
