/**
 * Launcher do server.js: reinicia automaticamente quando o processo encerra
 * (ex.: após POST /disconnect com WA_RESTART_ON_DISCONNECT=1).
 * Uso: node launcher.js — ou no monorepo: npm run dev (que chama dev:wa com este launcher).
 */
const { spawn } = require("child_process");
const path = require("path");

const DIR = __dirname;
const SERVER = path.join(DIR, "server.js");
const RESTART_DELAY_MS = 3000;

let child = null;

function run() {
  child = spawn(process.execPath, [SERVER], {
    cwd: DIR,
    stdio: "inherit",
    env: { ...process.env, WA_RESTART_ON_DISCONNECT: "1" },
  });
  child.on("exit", (code, signal) => {
    child = null;
    if (code === 0) {
      console.log("[launcher] Processo encerrado. Reiniciando em", RESTART_DELAY_MS / 1000, "s...");
      setTimeout(run, RESTART_DELAY_MS);
    } else {
      console.log("[launcher] Processo saiu com code=" + code + ", signal=" + signal + ". Reiniciando em", RESTART_DELAY_MS / 1000, "s...");
      setTimeout(run, RESTART_DELAY_MS);
    }
  });
}

process.on("SIGINT", () => {
  if (child) child.kill("SIGINT");
  process.exit(0);
});
process.on("SIGTERM", () => {
  if (child) child.kill("SIGTERM");
  process.exit(0);
});

run();
