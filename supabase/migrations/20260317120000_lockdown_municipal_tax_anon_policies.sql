-- Lockdown: remove políticas permissivas para anon em tabelas de Taxas/Impostos.
-- MOTIVO: anon key é publicável no frontend; RLS não pode permitir ALL para anon.
-- Mantém leitura para authenticated (dashboard) e escrita somente via service role / edge functions.

alter table public.municipal_tax_collection_runs enable row level security;
alter table public.municipal_tax_debts enable row level security;

-- Remove políticas inseguras do anon
drop policy if exists "anon_all_municipal_tax_collection_runs" on public.municipal_tax_collection_runs;
drop policy if exists "anon_all_municipal_tax_debts" on public.municipal_tax_debts;

-- Garante que anon NÃO consegue ler/escrever
drop policy if exists "anon_select_municipal_tax_collection_runs" on public.municipal_tax_collection_runs;
create policy "anon_select_municipal_tax_collection_runs"
  on public.municipal_tax_collection_runs for select to anon using (false);

drop policy if exists "anon_select_municipal_tax_debts" on public.municipal_tax_debts;
create policy "anon_select_municipal_tax_debts"
  on public.municipal_tax_debts for select to anon using (false);

-- Dashboard: usuários logados podem ler
drop policy if exists "authenticated_select_municipal_tax_collection_runs" on public.municipal_tax_collection_runs;
create policy "authenticated_select_municipal_tax_collection_runs"
  on public.municipal_tax_collection_runs for select to authenticated using (true);

drop policy if exists "authenticated_select_municipal_tax_debts" on public.municipal_tax_debts;
create policy "authenticated_select_municipal_tax_debts"
  on public.municipal_tax_debts for select to authenticated using (true);

-- Bloqueia escrita para authenticated por padrão (escrita deve ocorrer via service role/edge functions)
drop policy if exists "authenticated_write_municipal_tax_collection_runs" on public.municipal_tax_collection_runs;
create policy "authenticated_write_municipal_tax_collection_runs"
  on public.municipal_tax_collection_runs for all to authenticated using (false) with check (false);

drop policy if exists "authenticated_write_municipal_tax_debts" on public.municipal_tax_debts;
create policy "authenticated_write_municipal_tax_debts"
  on public.municipal_tax_debts for all to authenticated using (false) with check (false);

