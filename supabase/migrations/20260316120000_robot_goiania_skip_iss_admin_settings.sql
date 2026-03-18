-- Garante que o robô Goiânia (anon key) consiga ler a opção "Não capturar débitos de ISS".
-- 1) Insere a chave se não existir (default false = não ignorar ISS).
-- 2) RLS em admin_settings com política para anon SELECT (robô lê) e authenticated ALL (dashboard edita).

insert into public.admin_settings (key, value, updated_at)
values ('robot_goiania_skip_iss', 'false', now())
on conflict (key) do nothing;

alter table public.admin_settings enable row level security;

drop policy if exists "admin_settings_anon_select" on public.admin_settings;
create policy "admin_settings_anon_select"
  on public.admin_settings for select to anon using (true);

drop policy if exists "admin_settings_authenticated_all" on public.admin_settings;
create policy "admin_settings_authenticated_all"
  on public.admin_settings for all to authenticated using (true) with check (true);

comment on table public.admin_settings is 'Configurações do painel e robôs. anon pode SELECT (robô lê robot_goiania_skip_iss); authenticated pode tudo.';
