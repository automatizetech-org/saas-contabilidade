/**
 * Wrapper para o start.bat: sobe o PM2 e fica em pm2 logs.
 * Quando receber sinal de encerramento (SIGTERM/SIGINT/SIGBREAK), chama stop.bat e sai.
 * Use na tarefa do Agendador em vez do start.bat para que "Finalizar tarefa" pare tudo.
 *
 * Uso: node start-wrapper.js
 * (Ou configure a tarefa para: Programa = node, Argumentos = "C:\...\Servidor\start-wrapper.js", Iniciar em = C:\...\Servidor)
 */

const path = require("path");
const { spawn, spawnSync } = require("child_process");

const DIR = __dirname;
const STOP_BAT = path.join(DIR, "stop.bat");

function runStop() {
  try {
    spawnSync("cmd", ["/c", STOP_BAT], { cwd: DIR, stdio: "inherit", windowsHide: false });
  } catch (_) {}
}

function main() {
  const pm2Start = spawn("pm2", ["start", "ecosystem.config.cjs"], {
    cwd: DIR,
    stdio: "inherit",
    shell: true,
  });
  pm2Start.on("exit", (code) => {
    if (code !== 0) process.exit(code || 1);
  });

  pm2Start.on("close", () => {
    const pm2Logs = spawn("pm2", ["logs"], {
      cwd: DIR,
      stdio: "inherit",
      shell: true,
    });

    const cleanup = () => {
      runStop();
      try {
        pm2Logs.kill();
      } catch (_) {}
      process.exit(0);
    };

    process.on("SIGTERM", cleanup);
    process.on("SIGINT", cleanup);
    process.on("SIGBREAK", cleanup); // Windows: Ctrl+Break

    pm2Logs.on("exit", (code, signal) => {
      process.exit(signal ? 0 : code || 0);
    });
  });
}

main();
