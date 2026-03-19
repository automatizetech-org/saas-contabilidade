/**
 * Mata processos que estão escutando nas portas PORT_API e PORT_WHATSAPP (do .env).
 * Usado pelo stop.bat e pelo start-wrapper para liberar as portas.
 * No Windows: PowerShell + fallback netstat (Node parseia e taskkill); duas passadas com intervalo.
 */
require("./load-env.js");
const { execSync, spawnSync } = require("child_process");
const portApi = process.env.PORT_API || "3001";
const portWa = process.env.PORT_WHATSAPP || "3010";

function sleepSec(sec) {
  if (process.platform !== "win32") return;
  try {
    execSync(`timeout /t ${sec} /nobreak >nul`, { stdio: "ignore", windowsHide: true });
  } catch (_) {}
}

function killPortWin(port) {
  const portStr = String(port);
  try {
    execSync(
      `powershell -NoProfile -Command "Get-NetTCPConnection -LocalPort ${portStr} -ErrorAction SilentlyContinue | ForEach-Object { Stop-Process -Id $_.OwningProcess -Force -ErrorAction SilentlyContinue }"`,
      { stdio: "ignore", windowsHide: true, timeout: 5000 }
    );
  } catch (_) {}
  try {
    const out = execSync("netstat -ano", { encoding: "utf8", windowsHide: true, timeout: 5000 });
    const pids = new Set();
    for (const line of out.split(/\r?\n/)) {
      if (!line.includes(`:${portStr}`)) continue;
      const tokens = line.trim().split(/\s+/);
      const pid = tokens[tokens.length - 1];
      if (/^\d+$/.test(pid)) pids.add(pid);
    }
    for (const pid of pids) {
      try {
        spawnSync("taskkill", ["/F", "/PID", pid], { stdio: "ignore", windowsHide: true, timeout: 3000 });
      } catch (_) {}
    }
  } catch (_) {}
}

if (process.platform === "win32") {
  killPortWin(portApi);
  killPortWin(portWa);
  sleepSec(1);
  killPortWin(portApi);
  killPortWin(portWa);
  sleepSec(2);
}
