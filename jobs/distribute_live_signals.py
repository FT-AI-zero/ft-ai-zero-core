# -*- coding: utf-8 -*-
import os, time, sqlite3, json, traceback
from utils.config import LOG_DIR, STRATEGY_POOL_DB, SIGNAL_POOL_DB

LOG_PATH = os.path.join(LOG_DIR, "live_dist.log")
EXPIRE_SEC = int(os.getenv("LIVE_DIST_EXPIRE", "300"))   # 默认 5 分钟
SLEEP_SECONDS = float(os.getenv("LIVE_DIST_SLEEP", "2")) # 默认 2 秒
BATCH_LIMIT = int(os.getenv("LIVE_DIST_BATCH", "200"))

def log(msg: str, also_print: bool=False):
    line = f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}"
    if also_print:
        print(line)
    for _ in range(5):  # 最多重试5次
        try:
            os.makedirs(LOG_DIR, exist_ok=True)
            with open(LOG_PATH, "a", encoding="utf-8") as f:
                f.write(line + "\n")
            break
        except PermissionError:
            time.sleep(0.5)  # 等编辑器释放锁
        except Exception as e:
            # 兜底到临时文件，避免整个任务崩溃
            with open(os.path.join(LOG_DIR, "live_dist.fallback.log"), "a", encoding="utf-8") as f:
                f.write(f"{line}  [fallback due to {type(e).__name__}: {e}]\n")
            break

def allowed_gids():
    con = sqlite3.connect(STRATEGY_POOL_DB)
    try:
        rows = con.execute("SELECT param_group_id FROM allowlist").fetchall()
        return {int(r[0]) for r in rows}
    finally:
        con.close()

def main_loop():
    log("[LIVE-DIST] start", also_print=True)
    last_beat = 0
    while True:
        try:
            allow = allowed_gids()
            now_ts = int(time.time())

            moved = expired = scanned = 0
            with sqlite3.connect(SIGNAL_POOL_DB) as con:
                rows = con.execute(f"""
                    SELECT id, instId, COALESCE(period, interval), ts, close, vol, signal_type, meta
                    FROM signals
                    WHERE status='WAIT_SIMU'
                    ORDER BY ts ASC
                    LIMIT {BATCH_LIMIT}
                """).fetchall()

                scanned = len(rows)
                for sid, inst, period, ts, close, vol, sigtype, meta in rows:
                    try:
                        m = json.loads(meta) if meta else {}
                    except:
                        m = {}
                    gid = m.get("param_group_id") or m.get("gid")
                    if not gid:
                        con.execute("UPDATE signals SET status='SKIP_NO_GID' WHERE id=?", (sid,))
                        continue

                    # 过期控制
                    try:
                        sig_ts = int(ts or 0)
                    except:
                        sig_ts = 0
                    if now_ts - sig_ts > EXPIRE_SEC:
                        con.execute("UPDATE signals SET status='EXPIRED' WHERE id=?", (sid,))
                        expired += 1
                        continue

                    # 白名单 gating
                    if int(gid) in allow:
                        con.execute("UPDATE signals SET status='WAIT_LIVE' WHERE id=?", (sid,))
                        moved += 1
                    else:
                        # 白名单之外保持 WAIT_SIMU
                        pass

                con.commit()

            # 每轮都打印一条统计，便于观察
            log(f"[LIVE-DIST] scanned={scanned}, moved={moved}, expired={expired}, allow_cnt={len(allow)}", also_print=True)

        except Exception as e:
            log(f"[ERROR] {e}\n{traceback.format_exc()}", also_print=True)

        # 心跳节流
        time.sleep(SLEEP_SECONDS)

if __name__ == "__main__":
    main_loop()
