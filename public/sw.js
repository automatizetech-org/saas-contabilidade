/* Minimal SW para habilitar instalação (A2HS) no Android/Chrome.
   Não faz cache/offline de propósito. */

self.addEventListener("install", () => {
  self.skipWaiting();
});

self.addEventListener("activate", (event) => {
  event.waitUntil(self.clients.claim());
});

// Fetch handler “pass-through” (não intercepta respostas)
self.addEventListener("fetch", () => {});

