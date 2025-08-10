# utils/allowlist.py
import sqlite3
from utils.config import STRATEGY_POOL_DB

def is_gid_allowed(gid: int) -> bool:
    conn = sqlite3.connect(STRATEGY_POOL_DB)
    row = conn.execute(
        "SELECT 1 FROM allowlist WHERE param_group_id=? LIMIT 1",
        (gid,)
    ).fetchone()
    conn.close()
    return bool(row)
