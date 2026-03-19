@echo off
cd /d "%~dp0"
set "ORIGEM=%~dp0Servidor"
set "DEST=%USERPROFILE%\Documents\Servidor"

if not exist "%ORIGEM%" (
  echo Pasta nao encontrada: %ORIGEM%
  exit /b 1
)

echo Espelhando Servidor para Documents\Servidor...
echo Origem: %ORIGEM%
echo Destino: %DEST%
echo.

REM /E = subpastas; /XD = excluir dirs; /XF = excluir arquivo .env (nao sobrescrever config local)
robocopy "%ORIGEM%" "%DEST%" /E /XD node_modules .wwebjs_auth /XF .env /NFL /NDL /NJH /NJS

REM Remover .env antigos de subpastas (agora so existe um .env na raiz do Servidor)
if exist "%DEST%\server-api\.env" del "%DEST%\server-api\.env"
if exist "%DEST%\whatsapp-emissor\.env" del "%DEST%\whatsapp-emissor\.env"

REM Se nao existir .env no destino, copiar .env.example para .env
if not exist "%DEST%\.env" if exist "%DEST%\.env.example" (
  copy "%DEST%\.env.example" "%DEST%\.env" >nul
  echo Criado %DEST%\.env a partir de .env.example - edite com os valores da VM.
)

echo.
echo Pronto. Para instalar dependencias no destino:
echo   cd "%DEST%\server-api"   ^&^& npm install
echo   cd "%DEST%\whatsapp-emissor"   ^&^& npm install
pause
