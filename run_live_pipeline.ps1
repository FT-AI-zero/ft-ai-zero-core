param(
  [int]$SleepSec = 5
)

$ErrorActionPreference = "Stop"
$PSDefaultParameterValues['Out-File:Encoding'] = 'utf8'

# 环境变量（与 .bat 保持一致；在任务计划中也会继承）
$env:OKX_SKIP_TEST_POS = '1'  # 实盘改为 '0'
$env:PM_DRY_RUN        = '1'  # 实盘改为 '0'
$env:PYTHONIOENCODING  = 'utf-8'

$Log = "data\logs\pipeline.log"
$ts = { (Get-Date).ToString("yyyy-MM-dd HH:mm:ss") }

function Log([string]$msg){ "$([datetime]::Now.ToString('yyyy-MM-dd HH:mm:ss')) $msg" | Out-File -FilePath $Log -Append }

Write-Host "[$(& $ts)] START live pipeline"
Log "START live pipeline"

while ($true) {
  try {
    Log "ROUND start"
    python -m jobs.rollup_live_trades    *>> $Log
    python -m jobs.pnl_replay --once     *>> $Log
    python -m jobs.promote_by_pnl_live_v2*>> $Log
    python -m jobs.sync_allowlist        *>> $Log
  } catch {
    Log "[ERROR] $($_.Exception.Message)"
  }
  Log "SLEEP $SleepSec s"
  Start-Sleep -Seconds $SleepSec
}
