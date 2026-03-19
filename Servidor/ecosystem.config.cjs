/**
 * PM2: sobe os três serviços (WhatsApp, server-api, ngrok).
 * Configuração em .env (PORT_API, PORT_WHATSAPP, BASE_PATH, PM2_APPS).
 * Uso: pm2 start ecosystem.config.cjs
 */
require("./load-env.js");
const fs = require("fs");
const path = require("path");

const servidorDir = __dirname;
const PORT_API = process.env.PORT_API || "3001";
const PORT_WHATSAPP = process.env.PORT_WHATSAPP || "3010";
const BASE_PATH = process.env.BASE_PATH || "";

const ngrokInServidor = path.join(servidorDir, "Ngrok", "ngrok.exe");
const ngrokInDocuments = path.join(process.env.USERPROFILE || "", "Documents", "Ngrok", "ngrok.exe");
const ngrokPath = fs.existsSync(ngrokInServidor) ? ngrokInServidor : (fs.existsSync(ngrokInDocuments) ? ngrokInDocuments : null);
const hasLocalNgrok = !!ngrokPath;
const ngrokScript = hasLocalNgrok ? ngrokPath : path.join(servidorDir, "ngrok-wrapper.js");
const ngrokCwd = hasLocalNgrok ? path.dirname(ngrokPath) : servidorDir;

module.exports = {
  apps: [
    {
      name: "whatsapp-emissor",
      script: "server.js",
      cwd: path.join(servidorDir, "whatsapp-emissor"),
      env: {
        WA_RESTART_ON_DISCONNECT: "1",
        WA_SERVER_PORT: PORT_WHATSAPP,
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
        PORT: PORT_API,
        BASE_PATH,
        CONNECTOR_SECRET: process.env.CONNECTOR_SECRET || "",
        SUPABASE_URL: process.env.SUPABASE_URL || "",
        SUPABASE_SERVICE_ROLE_KEY: process.env.SUPABASE_SERVICE_ROLE_KEY || "",
      },
      restart_delay: 2000,
      max_restarts: 20,
    },
    {
      name: "ngrok",
      script: ngrokScript,
      args: hasLocalNgrok ? `http ${PORT_API}` : "",
      cwd: ngrokCwd,
      interpreter: hasLocalNgrok ? "none" : "node",
      restart_delay: 2000,
      max_restarts: 10,
    },
  ],
};
