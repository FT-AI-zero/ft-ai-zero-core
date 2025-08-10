# jobs/tools_scheduler.py
import sys, subprocess, time, datetime as dt
from utils.config import LOG_DIR

def run(job):
    ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] RUN -> {job}")
    try:
        # 直接调用 python -m module
        subprocess.run([sys.executable, "-m", job], check=False)
    except Exception as e:
        print(f"[ERR] {job}: {e}")

def next_daily(h, m):
    now = dt.datetime.now()
    target = now.replace(hour=h, minute=m, second=0, microsecond=0)
    if target <= now:
        target = target + dt.timedelta(days=1)
    return target

def next_hourly(minute, second=0):
    now = dt.datetime.now().replace(second=0, microsecond=0)
    hh = now.hour + (1 if now.minute >= minute else 0)
    target = now.replace(hour=hh % 24, minute=minute, second=second)
    if target <= dt.datetime.now():
        target = target + dt.timedelta(hours=1)
    return target

def next_every_n_minutes(n):
    now = dt.datetime.now().replace(second=0, microsecond=0)
    add = (n - (now.minute % n)) % n
    if add == 0: add = n
    return now + dt.timedelta(minutes=add)

def main():
    print(f"[scheduler] log -> {LOG_DIR}")
    # 计划时间点
    t_daily      = next_daily(0, 5)        # 每天 00:05
    t_hourly_pnl = next_hourly(10, 0)      # 每小时 xx:10 promote_by_pnl_live
    t_hourly_wh  = next_hourly(15, 0)      # 每小时 xx:15 sync_allowlist
    t_bandit     = next_every_n_minutes(10)# 每 10 分钟 bandit_update

    while True:
        now = dt.datetime.now()

        # 每日汇总（成交量候选 + 白名单同步）
        if now >= t_daily:
            run("jobs.clean_and_rollup")
            run("jobs.promote_by_volume")
            run("jobs.sync_allowlist")
            t_daily = next_daily(0, 5)

        # 每小时：按实盘收益推进候选
        if now >= t_hourly_pnl:
            run("jobs.promote_by_pnl_live")
            t_hourly_pnl = next_hourly(10, 0)

        # 每 10 分钟：Bandit 动态权重（方向代理）
        if now >= t_bandit:
            run("jobs.bandit_update")
            t_bandit = next_every_n_minutes(10)

        # 每小时：候选 → allowlist（叠加 volume/pnl 的来源）
        if now >= t_hourly_wh:
            run("jobs.sync_allowlist")
            t_hourly_wh = next_hourly(15, 0)

        time.sleep(20)

if __name__ == "__main__":
    main()
