# DS02 – Role C: kiểm tra máy trước khi demo / chấm (Windows PowerShell 5+ hoặc PowerShell 7)
#
#   powershell -ExecutionPolicy Bypass -File deploy\preflight.ps1
#
# Không sửa gì trên máy: chỉ in [OK] / [WARN] / [FAIL] và gợi ý cách xử lý.

$ErrorActionPreference = "Continue"
$repo = Split-Path -Parent $PSScriptRoot
$fail = 0

function Ok($msg)   { Write-Host "[OK]   $msg" -ForegroundColor Green }
function Warn($msg) { Write-Host "[WARN] $msg" -ForegroundColor Yellow }
function Bad($msg)  { Write-Host "[FAIL] $msg" -ForegroundColor Red; $script:fail++ }

Write-Host "DS02 preflight – repo: $repo"

# 1. Python launcher + thư viện
$py = Get-Command py -ErrorAction SilentlyContinue
if (-not $py) {
    Bad "Không thấy 'py' (Python launcher). Cài Python >= 3.10 từ python.org."
} else {
    $ver = & py -c "import sys; print('%d.%d' % sys.version_info[:2])"
    Ok "Python $ver"
    & py -c "import yaml" 2>$null
    if ($LASTEXITCODE -eq 0) { Ok "pyyaml" } else { Bad "Thiếu pyyaml: py -m pip install -r requirements.txt" }
    & py -c "import matplotlib" 2>$null
    if ($LASTEXITCODE -eq 0) { Ok "matplotlib" } else { Warn "Thiếu matplotlib (chỉ cần cho biểu đồ): py -m pip install -r requirements.txt" }
}

# 2. Cổng tracker + peer của demo và các kịch bản
$ports = @(8000) + (9001..9010) + (9101..9110) + (9301..9302)
$busy = @()
foreach ($p in $ports) {
    $c = Get-NetTCPConnection -LocalPort $p -State Listen -ErrorAction SilentlyContinue
    if ($c) { $busy += "$p (PID $($c[0].OwningProcess))" }
}
if ($busy.Count -eq 0) {
    Ok "Cổng 8000, 9001-9010, 9101-9110, 9301-9302 đều trống"
} else {
    Bad ("Cổng đang bị chiếm: " + ($busy -join ", ") + "  -> taskkill /PID <pid> /F")
}

# 3. Tiến trình python còn sót từ lần chạy trước
$left = Get-CimInstance Win32_Process -Filter "Name like 'python%'" -ErrorAction SilentlyContinue |
    Where-Object { $_.CommandLine -match "stub_peer|stub_tracker|src\.tracker|-m peer" }
if ($left) {
    Bad ("Còn tiến trình mô phỏng cũ: " + (($left | ForEach-Object { $_.ProcessId }) -join ", ") + "  -> Stop-Process -Id <pid>")
} else {
    Ok "Không còn tiến trình peer/tracker cũ"
}

# 4. OneDrive: log ghi vào <repo>\logs có thể bị khóa khi đang đồng bộ
if ($repo -match "OneDrive") {
    Warn "Repo nằm trong OneDrive. Tạm dừng đồng bộ hoặc chạy demo với: py demo\run_demo.py --logs-root `$env:TEMP\ds02-logs"
} else {
    Ok "Repo không nằm trong OneDrive"
}

# 5. Cửa sổ terminal đủ rộng cho dashboard (96 cột)
try {
    $w = $Host.UI.RawUI.WindowSize.Width
    if ($w -ge 100) { Ok "Terminal rộng $w cột" } else { Warn "Terminal chỉ rộng $w cột; phóng to >= 100 cột để dashboard không vỡ dòng" }
} catch { Warn "Không đọc được độ rộng terminal" }

# 6. QuickEdit của console cũ (click vào cửa sổ làm treo output)
try {
    $qe = (Get-ItemProperty -Path "HKCU:\Console" -Name QuickEdit -ErrorAction Stop).QuickEdit
    if ($qe -eq 1 -and -not $env:WT_SESSION) {
        Warn "QuickEdit đang bật: đừng click vào cửa sổ console khi demo (hoặc dùng Windows Terminal)"
    } else { Ok "QuickEdit không gây treo (hoặc đang dùng Windows Terminal)" }
} catch { Ok "Không có cấu hình QuickEdit riêng" }

Write-Host ""
if ($fail -eq 0) { Write-Host "Sẵn sàng. Chạy: py demo\run_demo.py" -ForegroundColor Green; exit 0 }
Write-Host "$fail mục FAIL – xử lý trước khi demo." -ForegroundColor Red
exit 1
