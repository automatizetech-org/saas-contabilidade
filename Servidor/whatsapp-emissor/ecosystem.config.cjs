/**
 * PM2: WhatsApp emissor (esta pasta). Restart automático ao desconectar.
 * Uso (dentro desta pasta): pm2 start ecosystem.config.cjs
 * Ou use o ecosystem da pasta Servidor para subir tudo (esta app + server-api).
 */
module.exports = {
  apps: [
    {
      name: "whatsapp-emissor",
      script: "server.js",
      cwd: __dirname,
      env: {
        WA_RESTART_ON_DISCONNECT: "1",
      },
      restart_delay: 3000,
      max_restarts: 50,
      min_uptime: "5s",
    },
  ],
};
