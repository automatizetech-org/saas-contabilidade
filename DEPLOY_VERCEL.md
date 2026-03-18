# Deploy Vercel

## 1. Vercel

Configure o projeto com:

- Framework Preset: `Vite`
- Install Command: `npm ci`
- Build Command: `npm run build`
- Output Directory: `dist`

## 2. Environment Variables

Defina no Vercel:

- `SUPABASE_URL`
- `SUPABASE_ANON_KEY`

Nao defina no frontend:

- `SUPABASE_SERVICE_ROLE_KEY`
- `CREATE_ADMIN_SECRET`
- `BOOTSTRAP_SECRET`
- `BTG_WEBHOOK_SECRET`

## 3. Supabase antes do deploy

Execute antes de publicar:

```powershell
supabase db push
supabase functions deploy create-user
supabase functions deploy update-user
supabase functions deploy get-users-admin
supabase functions deploy delete-user
supabase functions deploy primeiro-escritorio
supabase functions deploy office-server
supabase functions deploy btg-create-ir-charge
supabase functions deploy create-admin
```

Se usar webhook BTG, publique tambem:

```powershell
supabase functions deploy btg-ir-webhook
```

## 4. Supabase Auth

No painel do Supabase, revise:

- `Site URL`: dominio final do Vercel
- `Redirect URLs`: dominio final do Vercel e, se precisar, preview domains
- signup publico: desabilite se o onboarding for sempre administrativo
- templates de reset/invite

## 5. Seguranca

Antes do dominio publico:

- rotacione `CREATE_ADMIN_SECRET`
- rotacione `BOOTSTRAP_SECRET`
- rotacione `BTG_WEBHOOK_SECRET`
- remova usuarios de teste
- confirme que nao existe `service_role` em env do frontend

## 6. Smoke test

Depois do deploy:

1. abrir `/login`
2. fazer login com owner
3. abrir `/admin`
4. listar usuarios
5. criar um usuario temporario
6. excluir o usuario temporario
7. testar logout e login novamente
8. validar que rota protegida sem sessao volta para `/login`
