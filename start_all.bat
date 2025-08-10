@echo off
setlocal EnableExtensions EnableDelayedExpansion
title FT-AI Zero һ��������Paper+Live��
chcp 65001 >nul

REM ========= ��֤�ڰ���Ŀ¼���У�D:\ai\okx_bot =========
cd /d %~dp0
echo [cwd] %cd%

REM ȷ�� Python �ܴӵ�ǰĿ¼������
set PYTHONPATH=%cd%

REM ========= �ɿ��� =========
set RUN_PAPER=1
set RUN_LIVE=1
set RUN_COLLECTORS=1
set RUN_SIGNAL_GEN=1
set RUN_SCHEDULER=1
set RUN_PIPELINE=1
set RUN_ONCE_ROLLUP_SCORER=1
set RUN_HEALTH_CHECK=1

REM ========= ȫ�ֻ��� =========
set OKX_SKIP_TEST_POS=1
set PM_VERBOSE=1
set PM_AUTO_FOLLOW=1
set PIPELINE_SLEEP=5
set PYTHONIOENCODING=utf-8

REM ========= �Լ죺���ɵ��� =========
python -c "import sys;import importlib;importlib.import_module('core');importlib.import_module('jobs');importlib.import_module('collectors');importlib.import_module('strategy');importlib.import_module('utils');print('import OK')" || (
  echo [FATAL] ������ʧ�ܣ���ȷ�Ϯ�ǰĿ䛞� okx_bot �K���� __init__.py
  pause
  exit /b 1
)

REM ========= Ŀ¼׼�� =========
for %%D in (
  data\shared\dbs data\shared\jsons
  data\paper\dbs  data\paper\logs  data\paper\jsons  data\paper\models
  data\live\dbs   data\live\logs   data\live\jsons   data\live\models
  data\logs data\runtime
) do (
  if not exist "%%D" md "%%D"
)

echo [preflight] Ǩ��/�޸����ݿ�ṹ...

REM ---- ����� ----
python -m tools.migrate_pm_experience           1>nul 2>nul

REM ---- allowlist Ψһ���޸� + interval �� ----
python -m tools.migrate_allowlist_unique_fix    1>nul 2>nul
python -m tools.migrate_allowlist_add_interval  1>nul 2>nul

REM ---- signals/trades ��Ҫ�� ----
python -m tools.migrate_signals_add_cols        1>nul 2>nul
python -m tools.migrate_trades_add_action       1>nul 2>nul

REM ---- Ϊ paper / live �ֱ�ȷ�� strategy_pool ----
cmd /c "set FT_MODE=paper&& python -m tools.migrate_sp_allowlist" 1>nul 2>nul
cmd /c "set FT_MODE=live && python -m tools.migrate_sp_allowlist" 1>nul 2>nul

REM ---- �����Ͻű���������ɾ��----
python tools\migrate_sp_allowlist.py            1>nul 2>nul
python tools\migrate_review_schema.py           1>nul 2>nul

REM ---- �� paper.allowlist �����ݣ�����һ�ݵ� live�����Դ���----
python -m tools.mirror_allowlist_to_live        1>nul 2>nul

REM ---- ͨ��Ǩ�����������ڣ�----
python -m tools.db_migrator                     1>nul 2>nul

echo.
echo RUN_PAPER=%RUN_PAPER%  RUN_LIVE=%RUN_LIVE%
echo �����Ӷര�ڣ��ɼ� / �鱨 / �ź� / ���� / ��λ / ���� / ��ˮ��
echo.

REM ========= �ɼ���������=========
if "%RUN_COLLECTORS%"=="1" (
  start "collector"         cmd /k python -m collectors.super_collector
  start "intel_collector"   cmd /k python -m collectors.super_intel_collector
)

REM ========= �ź����ɣ�����=========
if "%RUN_SIGNAL_GEN%"=="1" (
  start "signal_generator"  cmd /k python -m strategy.signal_generator
)

REM ========= ���� =========
if "%RUN_PAPER%"=="1" (
  start "trade_engine(paper)" cmd /k cmd /c "set FT_MODE=paper&& set PM_DRY_RUN=1&& python -m core.trade_engine"
)
if "%RUN_LIVE%"=="1" (
  start "zero_engine(live)"   cmd /k cmd /c "set FT_MODE=live&& set PM_DRY_RUN=0&& python -m core.zero_engine"
  start "position_guard"      cmd /k cmd /c "set FT_MODE=live&& set PM_DRY_RUN=0&& python -m core.position_guard"
  start "live_dist"           cmd /k cmd /c "set FT_MODE=live&& set PM_DRY_RUN=0&& python -m jobs.distribute_live_signals"
)

REM ========= ��λ������ʵ����=========
start "position_manager"   cmd /k python -m jobs.position_manager
start "pm_auto_tuner" /min cmd /k python -m jobs.pm_auto_tuner

REM ========= ���� & ��ˮ�ߣ�����=========
if "%RUN_SCHEDULER%"=="1" (
  start "scheduler"          cmd /k python -m jobs.tools_scheduler
)
if "%RUN_PIPELINE%"=="1" (
  start "pipeline"   /min    cmd /k python -m jobs.runner_live_pipeline
)

REM ========= ����&���֣���ѡ��������ִ��һ�Σ�=========
if "%RUN_ONCE_ROLLUP_SCORER%"=="1" (
  start "rollup_once" /min   cmd /k python -m jobs.rollup_live_trades
  start "review_scorer" /min cmd /k python -m jobs.review_scorer
)

REM ========= ����Ѳ�죨��ѡ��=========
if "%RUN_HEALTH_CHECK%"=="1" (
  start "health_check" /min  cmd /k python -m jobs.health_check
)

echo.
echo === ������־�����Ƶ��ն�ִ�в鿴��===
echo powershell -Command "Get-Content data\paper\logs\trade_engine.log -Encoding utf8 -Tail 80"
echo powershell -Command "Get-Content data\live\logs\zero_engine.log   -Encoding utf8 -Tail 80"
echo powershell -Command "Get-Content data\logs\position_manager.log   -Encoding utf8 -Tail 80"
echo powershell -Command "Get-Content data\logs\pm_auto_tuner.log      -Encoding utf8 -Tail 80"
echo powershell -Command "Get-Content data\logs\pipeline.log           -Encoding utf8 -Tail 80"

echo.
echo All started.
pause
