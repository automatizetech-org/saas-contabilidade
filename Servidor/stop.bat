@echo off
cd /d "%~dp0"

echo Parando PM2 e todos os apps (server-api, whatsapp-emissor, ngrok)...
npx --yes pm2 stop all 2>nul
npx --yes pm2 delete all 2>nul
npx --yes pm2 kill 2>nul
node "%~dp0stop-ports.js" 2>nul
echo Concluido. Tudo parado.
