# e-CAC - Caixa Postal

Implementação consolidada em um único arquivo Python:

- `ecac_caixa_postal.py`

O arquivo contém embutida a lógica de:

- runtime e `.env` em cascata
- integração com dashboard / Supabase / `/api/robot-config`
- certificado digital e automação da janela nativa
- bootstrap do navegador Playwright
- login no e-CAC, troca de perfil e extração da Caixa Postal
- worker separado e interface PySide6

## Execução

UI:

```powershell
python "docs\ecac\caixa postal\ecac_caixa_postal.py"
```

CLI / orquestrado:

```powershell
python "docs\ecac\caixa postal\ecac_caixa_postal.py" --no-ui --job-mode
```

## Ajuste fino de seletores

Os seletores encapsulados que podem precisar ajuste no ambiente real estão dentro de `ecac_caixa_postal.py` nas constantes:

- `PROFILE_MENU_LABELS`
- `MAILBOX_ENTRY_LABELS`
- `MODAL_CLOSE_LABELS`
- `MESSAGE_ROW_SELECTORS`
- `NEXT_PAGE_LABELS`
- `DETAIL_CLOSE_LABELS`
