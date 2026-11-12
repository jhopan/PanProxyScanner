@echo off
rem ============================================================================
rem  Installer Pan ProxyScanner untuk Windows
rem
rem  Cara pakai (klik dua kali, atau dari CMD/PowerShell):
rem      install.bat
rem
rem  Non-interaktif (mis. untuk otomatisasi):
rem      set PORT=8787 && set SERVICE=yes && set RUN_NOW=yes && set NONINTERACTIVE=1 && install.bat
rem
rem  Skrip ini: mendeteksi arsitektur, mengunduh rilis TERBARU dari GitHub,
rem  memasang ke %LOCALAPPDATA%\PanProxyScanner, lalu (opsional) mendaftarkan
rem  Task Scheduler agar jalan sendiri saat login.
rem ============================================================================
setlocal EnableExtensions EnableDelayedExpansion
chcp 65001 >nul 2>&1

set "REPO=jhopan/PanProxyScanner"
set "STABLE_URL=https://github.com/%REPO%/releases/download/latest/panproxyscanner-windows-amd64.zip"
set "API_URL=https://api.github.com/repos/%REPO%/releases/latest"
set "INSTALL_DIR=%LOCALAPPDATA%\PanProxyScanner"
set "EXE=%INSTALL_DIR%\panproxyscanner.exe"
set "TASK_NAME=PanProxyScanner"

if not defined PORT set "PORT="
if not defined SERVICE set "SERVICE="
if not defined RUN_NOW set "RUN_NOW="
if not defined NONINTERACTIVE set "NONINTERACTIVE=0"

echo.
echo Pan ProxyScanner - installer Windows
echo ====================================
echo.

rem ---------------------------------------------------------------- deteksi ----
set "ARCH=%PROCESSOR_ARCHITECTURE%"
set "ARCH_NAME=amd64"
if /I "%ARCH%"=="ARM64" set "ARCH_NAME=amd64 lewat emulasi"
echo   arsitektur : %ARCH%  [%ARCH_NAME%]
echo   tujuan     : %INSTALL_DIR%
echo.

rem ---------------------------------------------------------------- tanya ------
if not defined PORT (
  set /p "PORT=Port panel yang dipakai [8787]: "
)
if "%PORT%"=="" set "PORT=8787"
echo %PORT%| findstr /R "^[1-9][0-9]*$" >nul
if errorlevel 1 (
  echo   PERINGATAN: port "%PORT%" tidak valid, memakai 8787.
  set "PORT=8787"
)
netstat -ano | findstr /R /C:":%PORT% .*LISTENING" >nul 2>&1
if not errorlevel 1 (
  echo   PERINGATAN: port %PORT% sedang dipakai proses lain.
  if /I "%NONINTERACTIVE%"=="1" (
    echo   (mode non-interaktif: tetap dilanjutkan, tapi scanner kemungkinan gagal bind)
  ) else (
    set /p "PORT=Masukkan port lain [8790]: "
    if "!PORT!"=="" set "PORT=8790"
  )
)

if not defined SERVICE (
  set /p "SERVICE=Jadikan layanan otomatis (jalan saat login)? (yes/no) [yes]: "
)
if "%SERVICE%"=="" set "SERVICE=yes"
if /I "%SERVICE%"=="y" set "SERVICE=yes"
if /I not "%SERVICE%"=="yes" set "SERVICE=no"

if not defined RUN_NOW (
  set /p "RUN_NOW=Jalankan sekarang? (yes/no) [yes]: "
)
if "%RUN_NOW%"=="" set "RUN_NOW=yes"
if /I "%RUN_NOW%"=="y" set "RUN_NOW=yes"
if /I not "%RUN_NOW%"=="yes" set "RUN_NOW=no"
echo.

rem ---------------------------------------------------------------- unduh -----
set "TMPZIP=%TEMP%\panproxyscanner-install.zip"
set "URL=%STABLE_URL%"
echo   mengunduh %URL%
powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "try { Invoke-WebRequest -Uri '%URL%' -OutFile '%TMPZIP%' -UseBasicParsing -ErrorAction Stop } catch { exit 3 }"
if errorlevel 1 (
  echo   nama stabil belum tersedia di rilis terbaru, mencari lewat GitHub API...
  for /f "usebackq delims=" %%U in (`powershell -NoProfile -ExecutionPolicy Bypass -Command ^
      "$r = Invoke-RestMethod -Uri '%API_URL%' -Headers @{ 'User-Agent'='panproxyscanner-installer' } -UseBasicParsing; $a = $r.assets | Where-Object { $_.name -like 'panproxyscanner-*windows-amd64.zip' } | Select-Object -First 1; if ($a) { $a.browser_download_url }"`) do set "URL=%%U"
  if "!URL!"=="" (
    echo   GAGAL: tidak menemukan aset rilis. Cek https://github.com/%REPO%/releases
    goto :gagal
  )
  echo   mengunduh !URL!
  powershell -NoProfile -ExecutionPolicy Bypass -Command ^
    "try { Invoke-WebRequest -Uri '!URL!' -OutFile '%TMPZIP%' -UseBasicParsing -ErrorAction Stop } catch { exit 3 }"
  if errorlevel 1 (
    echo   GAGAL: unduhan gagal.
    goto :gagal
  )
)

rem ---------------------------------------------------------------- pasang ----
if not exist "%INSTALL_DIR%" mkdir "%INSTALL_DIR%" >nul 2>&1
echo   memasang ke %INSTALL_DIR%
powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "try { Expand-Archive -Path '%TMPZIP%' -DestinationPath '%INSTALL_DIR%' -Force -ErrorAction Stop } catch { exit 4 }"
if errorlevel 1 (
  echo   GAGAL: gagal mengekstrak arsip. Tutup dulu proses panproxyscanner.exe, lalu jalankan installer lagi.
  goto :gagal
)
del "%TMPZIP%" >nul 2>&1
if not exist "%EXE%" (
  echo   GAGAL: biner tidak ditemukan setelah ekstrak.
  goto :gagal
)
for /f "delims=" %%V in ('"%EXE%" version 2^>nul') do set "VERSION=%%V"
echo   terpasang : !VERSION!
echo.

rem ---------------------------------------------------------------- layanan ----
rem Catatan: biner Go ini bukan Windows Service asli (tidak bicara SCM), jadi dipakai
rem Task Scheduler "saat login" yang lebih andal untuk aplikasi seperti ini.
if /I "%SERVICE%"=="yes" (
  echo   mendaftarkan Task Scheduler "%TASK_NAME%" (jalan otomatis saat login)
  schtasks /Create /TN "%TASK_NAME%" /TR "cmd /c cd /d \"%INSTALL_DIR%\" && \"%EXE%\" serve -addr 0.0.0.0:%PORT%" /SC ONLOGON /F >nul 2>&1
  if errorlevel 1 (
    echo   PERINGATAN: gagal mendaftarkan task. Jalankan installer ini sebagai Administrator.
  ) else (
    echo   task terdaftar. Jalankan segera: schtasks /Run /TN "%TASK_NAME%"
    echo   hapus      : schtasks /Delete /TN "%TASK_NAME%" /F
  )
  echo   catatan: Windows akan bertanya soal firewall saat pertama kali port dibuka.
  echo            izinkan akses jaringan privat agar HP di LAN bisa membuka panel.
) else (
  echo   layanan otomatis dilewati.
)

rem ---------------------------------------------------------------- jalankan --
if /I "%RUN_NOW%"=="yes" (
  start "" /min /D "%INSTALL_DIR%" "%EXE%" serve -addr 0.0.0.0:%PORT%
  echo   menjalankan scanner di port %PORT% ...
  powershell -NoProfile -ExecutionPolicy Bypass -Command ^
    "$ok=$false; for ($i=0; $i -lt 10; $i++) { Start-Sleep -Seconds 2; try { $r = Invoke-WebRequest -Uri 'http://127.0.0.1:%PORT%/api/health' -UseBasicParsing -TimeoutSec 5; Write-Host -NoNewline '  health: '; Write-Host $r.StatusCode; $ok=$true; break } catch {} }; if (-not $ok) { Write-Host '  PERINGATAN: health check gagal setelah 20 detik - cek firewall/antivirus' }"
)

echo.
echo Selesai.
echo   panel    : http://localhost:%PORT%  (dan http://IP-LAN:%PORT% dari HP)
echo   biner    : %EXE%
echo   data     : %INSTALL_DIR%  (database proxies.db)
echo   pengaturan: %USERPROFILE%\.panproxyscanner\settings.json
echo   update   : jalankan install.bat lagi (selalu mengambil rilis terbaru)
echo   hapus    : schtasks /Delete /TN "%TASK_NAME%" /F  ^&  rmdir /s /q "%INSTALL_DIR%"
echo.
echo login panel bawaan: admin / admin  (ganti di menu Scheduler - Akun ^& API)
echo.
exit /b 0

:gagal
echo.
echo Instalasi dibatalkan.
exit /b 1
