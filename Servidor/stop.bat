@echo off
cd /d "%~dp0"

REM Para todos os apps do PM2 e encerra o daemon.
REM Se a tarefa do Agendador estiver rodando start.bat (com pm2 logs), ela termina ao rodar este stop.
echo Parando PM2 e todos os apps (whatsapp-emissor, server-api, ngrok)...
pm2 stop all 2>nul
pm2 delete all 2>nul
pm2 kill 2>nul
echo Concluido. Tudo parado.
