/**
 * Wrapper para o start.bat: sobe o PM2 e fica em pm2 logs.
 * Configuração em .env (PM2_APPS, PORT_*). Ao finalizar a tarefa, chama stop.bat.
 *
 * Uso: node start-wrapper.js
 * Tarefa do Agendador: Programa = node, Argumentos = "C:\...\Servidor\start-wrapper.js", Iniciar em = C:\...\Servidor
 */
require("./load-env.js");
const path = require("path");
const { spawn, spawnSync, execSync } = require("child_process");

const DIR = __dirname;
const STOP_BAT = path.join(DIR, "stop.bat");
// npx na mesma pasta do node (funciona mesmo quando PATH nao tem pm2/npx)
const nodeDir = path.dirname(process.execPath);
const npxCmd = path.join(nodeDir, process.platform === "win32" ? "npx.cmd" : "npx");

let stopAlreadyRun = false;
function runStop() {
  if (stopAlreadyRun) return;
  stopAlreadyRun = true;
  try {
    spawnSync("cmd", ["/c", STOP_BAT], { cwd: DIR, stdio: "inherit", windowsHide: false });
  } catch (_) {}
}

function main() {
  const isWin = process.platform === "win32";
  const runPm2 = (args, opts = {}) => {
    const o = { cwd: DIR, windowsHide: false, ...opts };
    if (isWin) {
      execSync(`"${npxCmd}" --yes pm2 ${args}`, { ...o, shell: true });
    } else {
      spawnSync(npxCmd, ["--yes", "pm2", ...args.split(/\s+/)], { ...o });
    }
  };

  // Sempre roda stop primeiro para liberar portas (evita EADDRINUSE ao rodar start.bat direto)
  runStop();
  stopAlreadyRun = false;

  if (isWin) {
    try {
      require("child_process").execSync("timeout /t 5 /nobreak >nul", { stdio: "ignore", windowsHide: true });
    } catch (_) {}
    try {
      require("./stop-ports.js");
    } catch (_) {}
    try {
      require("child_process").execSync("timeout /t 2 /nobreak >nul", { stdio: "ignore", windowsHide: true });
    } catch (_) {}
  }
  try {
    runPm2("start ecosystem.config.cjs", { stdio: "inherit" });
  } catch (e) {
    process.exit(e.status != null ? e.status : 1);
  }

  const pm2Logs = isWin
    ? spawn(`"${npxCmd}" --yes pm2 logs`, [], { cwd: DIR, stdio: "inherit", shell: true, windowsHide: false })
    : spawn(npxCmd, ["--yes", "pm2", "logs"], { cwd: DIR, stdio: "inherit", shell: false });

  const cleanup = () => {
    runStop();
    try {
      pm2Logs.kill();
    } catch (_) {}
    process.exit(0);
  };

  process.on("SIGTERM", cleanup);
  process.on("SIGINT", cleanup);
  process.on("SIGBREAK", cleanup); // Windows: Ctrl+Break e às vezes ao finalizar tarefa

  // Ao sair por qualquer motivo (ex.: Agendador finaliza a tarefa), tenta parar o PM2
  process.on("exit", (code) => {
    runStop();
  });

  pm2Logs.on("exit", (code, signal) => {
    process.exit(signal ? 0 : code || 0);
  });
}

main();
