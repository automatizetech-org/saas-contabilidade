/**
 * PM2: sobe os três serviços (WhatsApp, server-api, ngrok) com um único comando.
 * Uso (na pasta Servidor): pm2 start ecosystem.config.cjs
 *
 * - whatsapp-emissor: porta 3010
 * - server-api: porta 3001
 * - ngrok: túnel para 3001 (interface em http://127.0.0.1:4040)
 *
 * Ngrok fica em Servidor/Ngrok (ex.: C:\Users\ROBO\Documents\Servidor\Ngrok\ngrok.exe).
 * Tarefa do Agendador: executar Servidor\start.bat (ou cmd com start.bat).
 */
const path = require("path");
const servidorDir = __dirname;
const ngrokDir = path.join(servidorDir, "Ngrok");
const ngrokPath = path.join(ngrokDir, "ngrok.exe");

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
      script: ngrokPath,
      args: "http 3001",
      cwd: ngrokDir,
      interpreter: "none",
      restart_delay: 2000,
      max_restarts: 10,
    },
  ],
};
