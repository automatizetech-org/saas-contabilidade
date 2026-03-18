@echo off
cd /d "%~dp0"

REM Porta do server-api — deve ser a mesma do .env (ex.: 3001)
set API_PORT=3001

REM Ngrok: mesma pasta, depois Documents\Ngrok, depois LocalAppData, depois PATH
set NGROK_EXE=
if exist "%~dp0ngrok.exe" set NGROK_EXE=%~dp0ngrok.exe
if not defined NGROK_EXE if exist "%USERPROFILE%\Documents\Ngrok\ngrok.exe" set NGROK_EXE=%USERPROFILE%\Documents\Ngrok\ngrok.exe
if not defined NGROK_EXE if exist "%LOCALAPPDATA%\ngrok\ngrok.exe" set NGROK_EXE=%LOCALAPPDATA%\ngrok\ngrok.exe
if not defined NGROK_EXE where ngrok >nul 2>&1 && set NGROK_EXE=ngrok

if not defined NGROK_EXE (
  echo [start.bat] ngrok nao encontrado. Coloque ngrok.exe nesta pasta ou instale: winget install ngrok.ngrok
) else (
  echo [start.bat] Iniciando ngrok na porta %API_PORT%...
  start "ngrok" /B "" "%NGROK_EXE%" http %API_PORT%
  timeout /t 5 /nobreak >nul
)

echo [start.bat] Iniciando API na porta %API_PORT%...
node index.js
