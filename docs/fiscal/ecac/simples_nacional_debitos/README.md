# e-CAC - Simples Nacional - Consulta Debitos

Script principal:

- `ecac_simples_debitos.py`

Suporta:

- login via certificado digital com UI Automation
- troca de perfil
- navegacao ate o PGDAS-D / DEFIS
- leitura estrutural de linhas de debito
- filtro de `numero_parcelamento == 0`
- persistencia em `result.json`

## Execucao

UI / navegador visivel:

```powershell
python "docs\fiscal\ecac\simples_nacional_debitos\ecac_simples_debitos.py" --company-name "EG FLEURY ASSESSORIA E SERVICOS LTDA" --company-document "37.197.978/0001-03"
```

Job mode:

```powershell
python "docs\fiscal\ecac\simples_nacional_debitos\ecac_simples_debitos.py" --no-ui --job-mode
```
