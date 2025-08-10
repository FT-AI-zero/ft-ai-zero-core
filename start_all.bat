@echo off
setlocal EnableExtensions EnableDelayedExpansion
title FT-AI Zero 一键启动（Paper+Live）
chcp 65001 >nul

REM ========= 保证在包根目录运行：D:\ai\okx_bot =========
cd /d %~dp0
echo [cwd] %cd%

REM 确保 Python 能从当前目录解析包
set PYTHONPATH=%cd%

REM ========= 可开关 =========
set RUN_PAPER=1
set RUN_LIVE=1
set RUN_COLLECTORS=1
set RUN_SIGNAL_GEN=1
set RUN_SCHEDULER=1
set RUN_PIPELINE=1
set RUN_ONCE_ROLLUP_SCORER=1
set RUN_HEALTH_CHECK=1

REM ========= 全局环境 =========
set OKX_SKIP_TEST_POS=1
set PM_VERBOSE=1
set PM_AUTO_FOLLOW=1
set PIPELINE_SLEEP=5
set PYTHONIOENCODING=utf-8

REM ========= 自检：包可导入 =========
python -c "import sys;import importlib;importlib.import_module('core');importlib.import_module('jobs');importlib.import_module('collectors');importlib.import_module('strategy');importlib.import_module('utils');print('import OK')" || (
  echo [FATAL] 包导入失败，请确认當前目錄為 okx_bot 並存在 __init__.py
  pause
  exit /b 1
)

REM ========= 目录准备 =========
for %%D in (
  data\shared\dbs data\shared\jsons
  data\paper\dbs  data\paper\logs  data\paper\jsons  data\paper\models
  data\live\dbs   data\live\logs   data\live\jsons   data\live\models
  data\logs data\runtime
) do (
  if not exist "%%D" md "%%D"
)

echo [preflight] 迁移/修复数据库结构...

REM ---- 经验库 ----
python -m tools.migrate_pm_experience           1>nul 2>nul

REM ---- allowlist 唯一键修复 + interval 列 ----
python -m tools.migrate_allowlist_unique_fix    1>nul 2>nul
python -m tools.migrate_allowlist_add_interval  1>nul 2>nul

REM ---- signals/trades 必要列 ----
python -m tools.migrate_signals_add_cols        1>nul 2>nul
python -m tools.migrate_trades_add_action       1>nul 2>nul

REM ---- 为 paper / live 分别确保 strategy_pool ----
cmd /c "set FT_MODE=paper&& python -m tools.migrate_sp_allowlist" 1>nul 2>nul
cmd /c "set FT_MODE=live && python -m tools.migrate_sp_allowlist" 1>nul 2>nul

REM ---- 兼容老脚本（可留可删）----
python tools\migrate_sp_allowlist.py            1>nul 2>nul
python tools\migrate_review_schema.py           1>nul 2>nul

REM ---- 若 paper.allowlist 有数据，镜像一份到 live（忽略错误）----
python -m tools.mirror_allowlist_to_live        1>nul 2>nul

REM ---- 通用迁移器（若存在）----
python -m tools.db_migrator                     1>nul 2>nul

echo.
echo RUN_PAPER=%RUN_PAPER%  RUN_LIVE=%RUN_LIVE%
echo 將啟動多窗口：采集 / 情报 / 信号 / 引擎 / 仓位 / 调度 / 流水线
echo.

REM ========= 采集器（共享）=========
if "%RUN_COLLECTORS%"=="1" (
  start "collector"         cmd /k python -m collectors.super_collector
  start "intel_collector"   cmd /k python -m collectors.super_intel_collector
)

REM ========= 信号生成（共享）=========
if "%RUN_SIGNAL_GEN%"=="1" (
  start "signal_generator"  cmd /k python -m strategy.signal_generator
)

REM ========= 引擎 =========
if "%RUN_PAPER%"=="1" (
  start "trade_engine(paper)" cmd /k cmd /c "set FT_MODE=paper&& set PM_DRY_RUN=1&& python -m core.trade_engine"
)
if "%RUN_LIVE%"=="1" (
  start "zero_engine(live)"   cmd /k cmd /c "set FT_MODE=live&& set PM_DRY_RUN=0&& python -m core.zero_engine"
  start "position_guard"      cmd /k cmd /c "set FT_MODE=live&& set PM_DRY_RUN=0&& python -m core.position_guard"
  start "live_dist"           cmd /k cmd /c "set FT_MODE=live&& set PM_DRY_RUN=0&& python -m jobs.distribute_live_signals"
)

REM ========= 仓位管理（单实例）=========
start "position_manager"   cmd /k python -m jobs.position_manager
start "pm_auto_tuner" /min cmd /k python -m jobs.pm_auto_tuner

REM ========= 调度 & 流水线（共享）=========
if "%RUN_SCHEDULER%"=="1" (
  start "scheduler"          cmd /k python -m jobs.tools_scheduler
)
if "%RUN_PIPELINE%"=="1" (
  start "pipeline"   /min    cmd /k python -m jobs.runner_live_pipeline
)

REM ========= 复盘&评分（可选：启动即执行一次）=========
if "%RUN_ONCE_ROLLUP_SCORER%"=="1" (
  start "rollup_once" /min   cmd /k python -m jobs.rollup_live_trades
  start "review_scorer" /min cmd /k python -m jobs.review_scorer
)

REM ========= 健康巡检（可选）=========
if "%RUN_HEALTH_CHECK%"=="1" (
  start "health_check" /min  cmd /k python -m jobs.health_check
)

echo.
echo === 常用日志（复制到终端执行查看）===
echo powershell -Command "Get-Content data\paper\logs\trade_engine.log -Encoding utf8 -Tail 80"
echo powershell -Command "Get-Content data\live\logs\zero_engine.log   -Encoding utf8 -Tail 80"
echo powershell -Command "Get-Content data\logs\position_manager.log   -Encoding utf8 -Tail 80"
echo powershell -Command "Get-Content data\logs\pm_auto_tuner.log      -Encoding utf8 -Tail 80"
echo powershell -Command "Get-Content data\logs\pipeline.log           -Encoding utf8 -Tail 80"

echo.
echo All started.
pause
