/**
 * PM2: sobe os tres servicos (WhatsApp, server-api, ngrok) com um unico comando.
 * Uso (na pasta Servidor): pm2 start ecosystem.config.cjs
 *
 * - whatsapp-emissor: porta 3010
 * - server-api: porta 3001
 * - ngrok: tunel para 3001 (interface em http://127.0.0.1:4040)
 *
 * Se existir Servidor/Ngrok/ngrok.exe, usa esse binario.
 * Caso contrario, usa o comando global "ngrok" instalado na maquina.
 * Tarefa do Agendador: executar Servidor\start.bat.
 */
const fs = require("fs");
const path = require("path");

const servidorDir = __dirname;
const ngrokDir = path.join(servidorDir, "Ngrok");
const ngrokPath = path.join(ngrokDir, "ngrok.exe");
const hasLocalNgrok = fs.existsSync(ngrokPath);
const ngrokScript = hasLocalNgrok ? ngrokPath : "ngrok";
const ngrokCwd = hasLocalNgrok ? ngrokDir : servidorDir;

module.exports = {
  apps: [
    {
      name: "whatsapp-emissor",
      script: "server.js",
      cwd: path.join(servidorDir, "whatsapp-emissor"),
      env: {
        WA_RESTART_ON_DISCONNECT: "1",
      },
      restart_delay: 3000,
      max_restarts: 50,
      min_uptime: "5s",
    },
    {
      name: "server-api",
      script: "index.js",
      cwd: path.join(servidorDir, "server-api"),
      interpreter: "node",
      env: {
        PORT: "3001",
      },
      restart_delay: 2000,
      max_restarts: 20,
    },
    {
      name: "ngrok",
      script: ngrokScript,
      args: "http 3001",
      cwd: ngrokCwd,
      interpreter: hasLocalNgrok ? "none" : undefined,
      restart_delay: 2000,
      max_restarts: 10,
    },
  ],
};
