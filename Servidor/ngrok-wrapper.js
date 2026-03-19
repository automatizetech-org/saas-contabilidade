/**
 * Wrapper para o PM2: executa "ngrok http PORT_API" usando o ngrok do PATH.
 * Porta lida do .env (PORT_API).
 */
require("./load-env.js");
const { spawn } = require("child_process");
const port = process.env.PORT_API || "3001";
const proc = spawn("ngrok", ["http", port], {
  stdio: "inherit",
  shell: false,
  windowsHide: false,
});
proc.on("error", (err) => {
  console.error("Erro ao iniciar ngrok:", err.message);
  console.error("Instale o ngrok (https://ngrok.com) ou coloque ngrok.exe em Servidor/Ngrok/ ou Documents/Ngrok/");
  process.exit(1);
});
proc.on("exit", (code, signal) => process.exit(signal ? 128 + signal : code));
