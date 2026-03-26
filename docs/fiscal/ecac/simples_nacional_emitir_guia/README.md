# e-CAC - Simples Nacional - Emitir Guia DAS

Script principal:

- `ecac_simples_emitir_guia.py`

O robo segue o mesmo padrao operacional do `docs/ecac/caixa postal/ecac_caixa_postal.py`:

- runtime com `.env` em cascata
- `job.json`, `result.json` e `heartbeat.json`
- integracao com dashboard / Supabase / `/api/robot-config`
- Playwright para automacao web
- UI Automation do Windows para a janela nativa do certificado
- modo manual e modo orquestrado
- output temporario por empresa em `data/output`
- limpeza automatica de artefatos antigos

## Execucao

UI / navegador visivel:

```powershell
python "docs\fiscal\ecac\simples_nacional_emitir_guia\ecac_simples_emitir_guia.py" --company-name "EG FLEURY ASSESSORIA E SERVICOS LTDA" --company-document "37.197.978/0001-03" --competencia "01/2026" --data-vencimento "20/02/2026" --dry-run
```

Job mode:

```powershell
python "docs\fiscal\ecac\simples_nacional_emitir_guia\ecac_simples_emitir_guia.py" --no-ui --job-mode
```

Geracao real somente sob flag explicita:

```powershell
python "docs\fiscal\ecac\simples_nacional_emitir_guia\ecac_simples_emitir_guia.py" --company-name "..." --company-document "..." --competencia "01/2026" --data-vencimento "20/02/2026" --real-generate
```
