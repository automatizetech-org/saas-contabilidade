# Instalação Para Outro Cliente

Este guia foi feito para quem não sabe programação.

A ideia é simples:

- No `PC`, você acessa os sites, confirma o dashboard e testa o login.
- Na `VM`, você deixa os serviços rodando para os robôs funcionarem.

Se seguir na ordem, funciona.

## Visão rápida

Você vai mexer em 3 lugares:

1. `Vercel`
   Onde fica o site do dashboard.
2. `Supabase`
   Onde ficam os dados, usuários, empresas, robôs e configurações.
3. `VM Windows`
   Onde os robôs realmente rodam.

## O que precisa antes

Separe isso antes de começar:

- Acesso ao projeto no `Vercel`
- Acesso ao projeto no `Supabase`
- Acesso à `VM Windows`
- A pasta do projeto nesta máquina
- A pasta `Servidor` para copiar para a VM
- A URL do site que será usada pelo cliente
- O caminho base que será usado na VM

Exemplo de caminho base:

```text
C:\Users\ROBO\Documents\EMPRESAS
```

## Parte 1: O que fazer no PC

### 1. Abrir o projeto no Vercel

Entre em:

- `https://vercel.com/dashboard`


Depois:

1. Faça login.
2. Abra o projeto do dashboard.
3. Clique em `Settings`.
4. Clique em `Environment Variables`.

Confira se existe a variável usada pelo frontend para falar com a API da VM.

A principal é:

```env
SERVER_API_URL
```

Se o cliente vai usar uma VM própria, essa URL precisa apontar para a API daquela VM.

Exemplo:

```env
SERVER_API_URL=https://url-da-api-do-cliente
```

Se também usar WhatsApp pela mesma API:

```env
WHATSAPP_API=https://url-da-api-do-cliente
```

Se você mudou alguma variável no Vercel:

1. Volte em `Deployments`
2. Clique nos `3 pontinhos`
3. Clique em `Redeploy`

### 2. Confirmar o site no navegador

Abra o site publicado no navegador.

Teste:

1. Login
2. Tela de empresas
3. Tela de Admin
4. Tela dos robôs
5. Tela do agendador

Se abrir tudo, a parte do site está ok.

### 3. Abrir o projeto no Supabase

Entre em:

- `https://supabase.com/dashboard`

Depois:

1. Faça login
2. Abra o projeto
3. Vá em `Table Editor`

Confira estas partes:

- Tabela `robots`
- Tabela `admin_settings`
- Tabela `companies`
- Tabela `company_robot_config`
- Tabela `schedule_rules`

### 4. Configurar o caminho base da VM no dashboard

O jeito mais fácil é pelo próprio dashboard, na área de Admin.

Procure a configuração de:

- `Caminho base na VM`

Preencha com o caminho real da VM.

Exemplo:

```text
C:\Users\ROBO\Documents\EMPRESAS
```

Importante:

- Esse caminho precisa existir ou a VM precisa ter permissão para criar.
- Esse caminho é o começo de todas as pastas das empresas.

### 5. Configurar a estrutura de pastas

No Admin, ajuste a `Estrutura de pastas`.

Exemplo:

- `Fiscal`
- `NFS`
- com regra de data `ano/mes/dia`

Se o robô estiver vinculado a `Fiscal/NFS` com data `ano/mes/dia`, ele vai salvar assim:

```text
C:\Users\ROBO\Documents\EMPRESAS\<empresa>\Fiscal\NFS\Emitidas\2026\03\17
```

ou

```text
C:\Users\ROBO\Documents\EMPRESAS\<empresa>\Fiscal\NFS\Recebidas\2026\03\17
```

### 6. Configurar cada robô no Admin

No dashboard, vá em `Admin > Robôs`.

Para cada robô, confira:

- Nome
- Departamento
- Estrutura de pastas
- Modo de data
- Modo de execução

No NFS, Sefaz, Certidões e outros, o importante é que cada um esteja apontando para o departamento correto.

### 7. Configurar empresas

Na tela das empresas, confirme:

- Empresa ativa
- Documento correto
- Senha do robô, quando usar login por senha
- Certificado e senha do certificado, quando usar certificado

Se faltar senha ou certificado, o robô pode pular a empresa.

## Parte 2: O que fazer na VM

Na VM, a estrutura é a mesma da pasta `Servidor` deste projeto.

Você vai deixar isso dentro de uma pasta parecida com:

```text
C:\Users\ROBO\Documents\Servidor
```

### 1. Copiar a pasta Servidor

Copie a pasta:

```text
Servidor
```

para a VM.

Ela precisa conter pelo menos:

- `server-api`
- `whatsapp-emissor`
- `start.bat`
- `stop.bat`
- `start-wrapper.js`
- `ecosystem.config.cjs`

Se usar túnel, copie também:

- `Servidor\Ngrok\ngrok.exe`

### 2. Instalar Node.js

Na VM, abra o navegador e entre em:

- `https://nodejs.org`

Baixe a versão `LTS`.

Instale clicando em `Next` até o final.

Depois confira:

1. Abra o `Prompt de Comando`
2. Digite:

```bat
node -v
```

Depois:

```bat
npm -v
```

Se aparecer a versão, está instalado.

### 3. Instalar PM2 na VM

No `Prompt de Comando`, rode:

```bat
npm install -g pm2
```

Depois teste:

```bat
pm2 -v
```

### 4. Instalar dependências da API e do WhatsApp

Abra o `Prompt de Comando`.

Entre na pasta do servidor:

```bat
cd C:\Users\ROBO\Documents\Servidor
```

Depois rode:

```bat
cd server-api
npm install
```

Volte:

```bat
cd ..
```

Agora rode:

```bat
cd whatsapp-emissor
npm install
```

### 5. Configurar o .env da VM

Na VM, o principal `.env` fica em:

```text
Servidor\server-api\.env
```

Se já existir um `.env` de outra máquina funcionando, copie e só ajuste o necessário.

Confira estas variáveis:

```env
PORT=3001
WHATSAPP_BACKEND_URL=http://localhost:3010
SUPABASE_URL=...
SUPABASE_ANON_KEY=...
SUPABASE_SERVICE_ROLE_KEY=...
CONNECTOR_SECRET=...
BASE_PATH=C:\Users\ROBO\Documents\EMPRESAS
```

Observação:

- Mesmo usando o caminho vindo do dashboard, deixe `BASE_PATH` preenchido no `.env` como segurança.
- Assim, se a API do dashboard falhar por algum motivo, ainda existe fallback.

- `CONNECTOR_SECRET` Ã© o segredo exibido uma Ãºnica vez no wizard do primeiro escritÃ³rio. Guarde e coloque no `.env` da VM.

### 6. Criar a pasta base da VM

Na VM, confirme que existe a pasta base.

Exemplo:

```text
C:\Users\ROBO\Documents\EMPRESAS
```

Se não existir:

1. Abra o Explorador de Arquivos
2. Vá até `C:\Users\ROBO\Documents`
3. Crie a pasta `EMPRESAS`

### 7. Subir tudo manualmente uma vez

Ainda na VM, no `Prompt de Comando`, entre em:

```bat
cd C:\Users\ROBO\Documents\Servidor
```

Rode:

```bat
pm2 start ecosystem.config.cjs
```

Depois confira:

```bat
pm2 list
```

Você deve ver:

- `whatsapp-emissor`
- `server-api`
- `ngrok` (se estiver usando)

Se quiser salvar o PM2 para subir depois:

```bat
pm2 save
```

### 8. Testar se a API está viva

Na VM, abra o navegador e teste:

```text
http://localhost:3001/health
```

Se aparecer algo como:

```json
{"ok":true}
```

está certo.

### 9. Testar se a configuração do robô responde

Ainda na VM, teste no navegador:

```text
http://localhost:3001/api/robot-config?technical_id=nfs_padrao
```

ou o `technical_id` do robô que você quer testar.

Tem que aparecer um JSON com dados como:

- `base_path`
- `segment_path`
- `date_rule`

Se abrir isso, o robô está conseguindo enxergar a configuração do dashboard.

## Parte 3: Agendador de Tarefas da VM

Na VM, você inicia pelo `Agendador de Tarefas do Windows`.

### 1. Abrir o Agendador

No menu iniciar da VM, pesquise:

- `Agendador de Tarefas`

Abra.

### 2. Criar a tarefa

Clique em:

- `Criar Tarefa`

### 3. Aba Geral

Preencha:

- Nome da tarefa: `Fleury Servidor`

Marque:

- `Executar estando o usuário conectado`

Use o usuário da VM que realmente roda os robôs.

### 4. Aba Disparadores

Clique em `Novo...`

Escolha:

- `Ao fazer logon`

Se quiser, adicione também:

- `Ao iniciar o computador`

### 5. Aba Ações

Clique em `Novo...`

Preencha exatamente assim:

- `Ação`: `Iniciar um programa`
- `Programa/script`:

```text
C:\Users\ROBO\Documents\Servidor\start.bat
```

- `Iniciar em`:

```text
C:\Users\ROBO\Documents\Servidor
```

Isso é exatamente o formato da sua imagem.

### 6. Aba Condições

Desmarque o que puder atrapalhar.

Se houver opção parecida com:

- `Iniciar a tarefa somente se o computador estiver ligado na energia`

deixe desmarcada.

### 7. Aba Configurações

Marque opções parecidas com:

- `Permitir que a tarefa seja executada sob demanda`
- `Se a tarefa falhar, reiniciar`

### 8. Testar a tarefa

Depois de salvar:

1. Clique com o botão direito na tarefa
2. Clique em `Executar`

Se estiver certo, ela deve iniciar o `Servidor\start.bat`.

## Parte 4: Teste final

Agora faça este teste simples:

### No PC

1. Acesse o site do dashboard
2. Faça login
3. Vá em `Admin`
4. Confirme:
   `Caminho base na VM`
5. Confirme:
   `Estrutura de pastas`
6. Confirme:
   `Robôs`

### Na VM

1. Confirme que o `server-api` está no ar
2. Confirme que o `whatsapp-emissor` está no ar
3. Confirme que a tarefa do Windows está criada

### No dashboard

1. Execute um robô manualmente
2. Veja se ele cria a pasta da empresa
3. Veja se salva no lugar certo

Exemplo esperado:

```text
C:\Users\ROBO\Documents\EMPRESAS\<empresa>\Fiscal\NFS\Recebidas\2026\03\17
```

ou

```text
C:\Users\ROBO\Documents\EMPRESAS\<empresa>\Fiscal\NFS\Emitidas\2026\03\17
```

## Parte 5: Se der problema

### O site não abre

Verifique:

- Projeto no `Vercel`
- Último deploy
- Variáveis do frontend

### O robô fala que não acha o caminho

Verifique:

1. `Caminho base na VM` no Admin
2. Se a API da VM está aberta
3. Se esta URL abre na VM:

```text
http://localhost:3001/api/robot-config?technical_id=nfs_padrao
```

### O robô abre mas não entra nas empresas

Verifique:

- Senha da empresa
- Certificado
- Senha do certificado
- Empresa ativa

### O servidor não sobe pela tarefa do Windows

Verifique:

- Se o caminho do `start.bat` está correto
- Se o campo `Iniciar em` está preenchido
- Se o usuário da tarefa é o usuário correto da VM

## Resumo curto

Se quiser fazer do jeito mais simples possível:

1. No `PC`, configure o dashboard e confirme Vercel + Supabase.
2. Na `VM`, copie a pasta `Servidor`.
3. Instale `Node.js`.
4. Instale `PM2`.
5. Rode `npm install` em `server-api` e `whatsapp-emissor`.
6. Ajuste o `.env` da VM.
7. Crie a pasta base.
8. Teste `pm2 start ecosystem.config.cjs`.
9. Crie a tarefa no `Agendador de Tarefas` apontando para `Servidor\start.bat`.
10. Teste um robô pelo dashboard.

Se esses 10 passos passarem, está pronto para o cliente usar.
