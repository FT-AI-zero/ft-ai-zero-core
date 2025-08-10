# jobs/sync_allowlist.py
import sqlite3, datetime as dt
from pathlib import Path
from typing import List, Tuple

# 关键：把需要的常量都引进来
from utils.config import DATA_DIR, MODE, AI_PARAMS_DB

# ---------- 路径解析：永远写入 data/{paper|live}/dbs/strategy_pool.db ----------
def _sp_db_path() -> str:
    base = Path(DATA_DIR)
    root_data = base.parent if base.name in ("shared", "paper", "live") else base
    sub = "live" if MODE == "live" else "paper"
    db = root_data / sub / "dbs" / "strategy_pool.db"
    db.parent.mkdir(parents=True, exist_ok=True)
    return str(db)

# ---------- 允许名单表结构（带唯一索引） ----------
def _ensure_allowlist_schema(db_path: str):
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS allowlist(
        param_group_id INTEGER NOT NULL,
        "window"       TEXT    NOT NULL,
        score          REAL    NOT NULL,
        trades         INTEGER NOT NULL,
        source         TEXT    DEFAULT '',
        weight         REAL    DEFAULT 1.0,
        updated_at     TEXT
    );
    """)
    cur.execute("""CREATE UNIQUE INDEX IF NOT EXISTS ux_allowlist_pg_win
                   ON allowlist(param_group_id, "window")""")
    con.commit()
    con.close()

# ---------- UPDATE→INSERT（避开 ON CONFLICT 的挑剔） ----------
def _upsert_allowlist(db_path: str, items: List[Tuple[int,str,float,int,str,float]]):
    """
    items: [(param_group_id, window, score, trades, source, weight), ...]
    """
    _ensure_allowlist_schema(db_path)
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    now = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    up_cnt = 0
    for pg, win, score, trades, source, weight in items:
        cur.execute("""
            UPDATE allowlist
               SET score=?, trades=?, source=?, weight=?, updated_at=?
             WHERE param_group_id=? AND "window"=?;
        """, (float(score), int(trades), source or "", float(weight or 1.0), now, int(pg), win))
        if cur.rowcount == 0:
            cur.execute("""
                INSERT INTO allowlist(param_group_id, "window", score, trades, source, weight, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?);
            """, (int(pg), win, float(score), int(trades), source or "", float(weight or 1.0), now))
        up_cnt += 1

    con.commit()
    top = cur.execute("""
        SELECT param_group_id, "window", ROUND(score,6), trades, source, weight, updated_at
          FROM allowlist
      ORDER BY updated_at DESC, score DESC
         LIMIT 20;
    """).fetchall()
    con.close()
    return up_cnt, top

# ---------- 读取 AI DB 里的候选，自动兼容多来源 ----------
def _table_exists(con: sqlite3.Connection, name: str) -> bool:
    return con.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?;", (name,)).fetchone()[0] > 0

def _read_candidates(ai_db: str) -> List[Tuple[int,str,float,int,str,float]]:
    """
    汇总 candidates / pnl_live_true 等来源，去重保分高
    返回 [(pg, window, score, trades, source, weight)]
    """
    con = sqlite3.connect(ai_db)
    out = {}

    # 来源1：通用 candidates（包含 volume/pnl 提名）
    if _table_exists(con, "candidates"):
        for pg, win, trades, score, source, _ts in con.execute(
            "SELECT param_group_id, window, trades, score, COALESCE(source,'') AS source, created_at FROM candidates ORDER BY rowid DESC LIMIT 500"
        ):
            key = (int(pg), str(win))
            val = (float(score or 0), int(trades or 0), source or "candidates", 1.0)
            if (key not in out) or (val[0] > out[key][0]):
                out[key] = val

    # 来源2：实盘 PnL 真阳性（如果存在）
    if _table_exists(con, "pnl_live_true"):
        for pg, win, trades, score, _ts in con.execute(
            "SELECT param_group_id, window, trades, score, created_at FROM pnl_live_true ORDER BY rowid DESC LIMIT 500"
        ):
            key = (int(pg), str(win))
            val = (float(score or 0), int(trades or 0), "pnl_live_true", 1.0)
            if (key not in out) or (val[0] > out[key][0]):
                out[key] = val

    con.close()

    items = []
    for (pg, win), (score, trades, source, weight) in out.items():
        items.append((pg, win, score, trades, source, weight))
    return items

def main():
    AI_DB = AI_PARAMS_DB            # <- 之前 NameError 就是这里没 import
    SP_DB = _sp_db_path()

    print(f"[DB] AI : {AI_DB}")
    print(f"[DB] SP : {SP_DB}")

    items = _read_candidates(AI_DB)
    up_cnt, top = _upsert_allowlist(SP_DB, items)

    print(f"[SYNC] upserted {up_cnt} allowlist rows.")
    for r in top[:20]:
        print(" ", r)

if __name__ == "__main__":
    main()
