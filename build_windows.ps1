$ErrorActionPreference = "Stop"
Push-Location $PSScriptRoot
try {
    py -m venv .build-venv
    if ($LASTEXITCODE -ne 0) { throw "Virtual environment creation failed" }
    $python = ".\.build-venv\Scripts\python.exe"
    & $python -m pip install --upgrade pip
    if ($LASTEXITCODE -ne 0) { throw "pip upgrade failed" }
    & $python -m pip install .\shared .\client "pyinstaller>=6.10"
    if ($LASTEXITCODE -ne 0) { throw "Client dependency installation failed" }
    & $python -m PyInstaller --noconfirm --clean file-sharing.spec
    if ($LASTEXITCODE -ne 0) { throw "Client executable build failed" }

    $artifact = Resolve-Path .\dist\FileSharing.exe
    $hash = Get-FileHash $artifact -Algorithm SHA256
    "$($hash.Hash.ToLower())  FileSharing.exe" | Set-Content -Encoding ascii .\dist\FileSharing.exe.sha256
    Write-Host "Built $artifact"
    Write-Host "SHA256 $($hash.Hash.ToLower())"
} finally {
    Pop-Location
}
