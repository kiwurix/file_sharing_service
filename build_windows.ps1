$ErrorActionPreference = "Stop"

py -m venv .build-venv
& .\.build-venv\Scripts\python.exe -m pip install --upgrade pip
& .\.build-venv\Scripts\python.exe -m pip install -r requirements.txt -r requirements-dev.txt
& .\.build-venv\Scripts\python.exe -m PyInstaller --noconfirm --clean file-sharing.spec

$artifact = Resolve-Path .\dist\FileSharing.exe
$hash = Get-FileHash $artifact -Algorithm SHA256
"$($hash.Hash.ToLower())  FileSharing.exe" | Set-Content -Encoding ascii .\dist\FileSharing.exe.sha256
Write-Host "Built $artifact"
Write-Host "SHA256 $($hash.Hash.ToLower())"
