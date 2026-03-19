/**
 * Carrega o .env da pasta Servidor em process.env (para ecosystem, ngrok-wrapper, stop-ports).
 * Uso: require('./load-env.js') no início do script.
 */
const path = require("path");
const fs = require("fs");
const dir = __dirname;
const envPath = path.join(dir, ".env");
if (fs.existsSync(envPath)) {
  const content = fs.readFileSync(envPath, "utf-8");
  for (const line of content.split("\n")) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) continue;
    const m = trimmed.match(/^([A-Za-z_][A-Za-z0-9_]*)=(.*)$/);
    if (m) process.env[m[1]] = m[2].replace(/^["']|["']$/g, "").trim();
  }
}
module.exports = process.env;
