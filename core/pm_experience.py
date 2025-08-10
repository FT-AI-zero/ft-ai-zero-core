# core/pm_experience.py
import os, json, sqlite3, datetime
from utils.config import DB_DIR

DB_PATH = os.path.join(DB_DIR, "pm_experience.db")

def _utcnow_iso():
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat()

def ensure_schema():
    os.makedirs(DB_DIR, exist_ok=True)
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS experiences(
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ts TEXT NOT NULL,              -- UTC ISO
      instId TEXT,
      state_json TEXT NOT NULL,      -- 市场/仓位状态
      action_json TEXT NOT NULL,     -- 决策
      reward REAL DEFAULT 0.0,       -- 即时收益(可先置0)
      info TEXT                      -- 备注
    )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_exp_ts ON experiences(ts)")
    con.commit(); con.close()

def add_experience(instId:str, state:dict, action:dict, reward:float=0.0, info:str=""):
    ensure_schema()
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
    INSERT INTO experiences(ts, instId, state_json, action_json, reward, info)
    VALUES(?,?,?,?,?,?)
    """, (_utcnow_iso(), instId, json.dumps(state, ensure_ascii=False),
          json.dumps(action, ensure_ascii=False), float(reward), info))
    con.commit(); con.close()
