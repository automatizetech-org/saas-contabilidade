@echo off
cd /d "%~dp0"

REM Sobe PM2 com ecosystem.config.cjs: whatsapp-emissor + server-api + ngrok (tudo junto).
REM Usa o wrapper para que "Finalizar tarefa" no Agendador chame o stop (para tudo).
REM Se a tarefa do Agendador rodar diretamente "node start-wrapper.js", ao clicar em Finalizar o stop roda.
node "%~dp0start-wrapper.js"
