# e-CAC - Simples Nacional - Consulta Extratos e DEFIS

Script principal:

- `ecac_simples_consulta_extratos_defis.py`

Suporta:

- login com certificado digital via UI Automation
- troca de perfil de acesso
- entrada em `PGDAS-D e Defis 2018`
- inventario de extratos por competencia
- inventario / tentativa de download de DEFIS declaracao e recibo
- `job/result/heartbeat`
- output temporario por empresa

## Execucao

UI / navegador visivel:

```powershell
python "docs\fiscal\ecac\simples_nacional_consulta_extratos_defis\ecac_simples_consulta_extratos_defis.py" --company-name "EG FLEURY ASSESSORIA E SERVICOS LTDA" --company-document "37.197.978/0001-03"
```

Com tentativa de download real:

```powershell
python "docs\fiscal\ecac\simples_nacional_consulta_extratos_defis\ecac_simples_consulta_extratos_defis.py" --company-name "..." --company-document "..." --real-download
```

Job mode:

```powershell
python "docs\fiscal\ecac\simples_nacional_consulta_extratos_defis\ecac_simples_consulta_extratos_defis.py" --no-ui --job-mode
```
