-- Políticas RLS para o robô Goiânia Taxas e Impostos (anon key).
-- Corrige: new row violates row-level security policy for table "municipal_tax_collection_runs"

alter table public.municipal_tax_collection_runs enable row level security;
alter table public.municipal_tax_debts enable row level security;

drop policy if exists "anon_all_municipal_tax_collection_runs" on public.municipal_tax_collection_runs;
create policy "anon_all_municipal_tax_collection_runs"
  on public.municipal_tax_collection_runs for all to anon using (true) with check (true);

drop policy if exists "anon_all_municipal_tax_debts" on public.municipal_tax_debts;
create policy "anon_all_municipal_tax_debts"
  on public.municipal_tax_debts for all to anon using (true) with check (true);
