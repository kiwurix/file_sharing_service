from PyInstaller.utils.hooks import collect_data_files, collect_dynamic_libs


datas = collect_data_files("tkinterdnd2")
binaries = collect_dynamic_libs("tkinterdnd2")

a = Analysis(
    ["client/src/file_sharing_client/__main__.py"],
    pathex=[],
    binaries=binaries,
    datas=datas,
    hiddenimports=["tkinterdnd2"],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name="FileSharing",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
