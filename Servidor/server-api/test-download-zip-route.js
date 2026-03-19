/**
 * Testa se a rota POST /api/documents/download-zip-by-paths está registrada no server-api.
 * Resposta esperada: 401 (não autorizado) = rota existe; 404 = rota não existe.
 * Execute: node test-download-zip-route.js
 * Requer: server-api rodando em localhost:3001 (ou PORT_API do .env)
 */
import dotenv from "dotenv";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: path.resolve(__dirname, "..", ".env") });

const PORT = process.env.PORT_API || process.env.PORT || 3001;
const BASE = `http://127.0.0.1:${PORT}`;

async function testRoute(pathToTry, body) {
  const res = await fetch(`${BASE}${pathToTry}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body ?? { items: [] }),
  });
  const text = await res.text();
  let json;
  try {
    json = JSON.parse(text);
  } catch {
    json = { raw: text.slice(0, 200) };
  }
  return { status: res.status, statusText: res.statusText, body: json };
}

async function main() {
  console.log("Testando server-api em", BASE, "\n");

  const paths = [
    "/api/documents/download-zip-by-paths",
    "/documents/download-zip-by-paths",
    "/api/api/documents/download-zip-by-paths",
  ];

  for (const p of paths) {
    try {
      const result = await testRoute(p, { items: [] });
      const ok = result.status !== 404;
      console.log(
        ok ? "[OK]" : "[404]",
        "POST",
        p,
        "->",
        result.status,
        result.body?.error || result.body?.raw || ""
      );
      if (result.status === 400 && result.body?.error?.includes("Nenhum arquivo")) {
        console.log("  -> Rota encontrada e handler executou (400 = body vazio).");
      }
      if (result.status === 401) {
        console.log("  -> Rota encontrada (401 = falta auth).");
      }
    } catch (e) {
      console.log("[ERRO]", "POST", p, "->", e.message);
    }
  }

  console.log("\nInterpretação:");
  console.log("  - 404 em todos: processo na porta 3001 é antigo. Mate o processo e suba de novo (ex.: start.bat ou pm2 restart server-api).");
  console.log("  - 500 'Conector não configurado': rota existe; suba o Servidor com start.bat para carregar CONNECTOR_SECRET do .env.");
  console.log("  - 401: rota existe e conector configurado (falta JWT do usuário na chamada).");
  console.log("  - 400 'Nenhum arquivo': rota e auth OK (body vazio no teste).");
}

main();
