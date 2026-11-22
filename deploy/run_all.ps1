# DS02 – Role C: chạy toàn bộ phần đánh giá trên một máy Windows
#
#   powershell -ExecutionPolicy Bypass -File deploy\run_all.ps1                                  # peer của B + tracker của A
#   powershell -ExecutionPolicy Bypass -File deploy\run_all.ps1 -Peer src/sim/stub_peer.py      # StubPeer của C
#
# Thứ tự: preflight -> bộ kiểm thử (C5) -> 5 thí nghiệm (C6) -> demo (C7).
# Log ghi vào %TEMP%\ds02-logs để tránh OneDrive khóa file; CSV/biểu đồ/analysis.md ghi vào results\.

param(
    [string]$Peer = "-m peer",
    [string]$Tracker = "-m src.tracker",
    [switch]$SkipDemo
)

$repo = Split-Path -Parent $PSScriptRoot
Set-Location $repo
$logs = Join-Path $env:TEMP "ds02-logs"

& powershell -ExecutionPolicy Bypass -File (Join-Path $PSScriptRoot "preflight.ps1")
if ($LASTEXITCODE -ne 0) { Write-Host "Preflight FAIL – dừng."; exit 1 }

Write-Host "`n=== C5: py tests/e2e/run_all.py ===" -ForegroundColor Cyan
& py tests/e2e/run_all.py --peer-script $Peer --tracker-script $Tracker
$tests = $LASTEXITCODE

Write-Host "`n=== C6: py src/sim/run_experiments.py ===" -ForegroundColor Cyan
& py src/sim/run_experiments.py --peer-script $Peer --tracker-script $Tracker --logs-root $logs
$exp = $LASTEXITCODE

$demo = 0
if (-not $SkipDemo) {
    Write-Host "`n=== C7: py demo/run_demo.py ===" -ForegroundColor Cyan
    & py demo/run_demo.py --peer-script $Peer --tracker-script $Tracker --logs-root $logs
    $demo = $LASTEXITCODE
}

Write-Host "`nKết quả: tests=$tests experiments=$exp demo=$demo (0 = OK)"
if ($tests -ne 0 -or $exp -ne 0 -or $demo -ne 0) { exit 1 }
exit 0
