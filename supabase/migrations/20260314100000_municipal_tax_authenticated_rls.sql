-- Permite ao dashboard (usuário logado = role authenticated) ler débitos e runs.
-- Sem isso, o dash não consegue captar os dados do Supabase na tela Taxas e Impostos.

drop policy if exists "authenticated_select_municipal_tax_collection_runs" on public.municipal_tax_collection_runs;
create policy "authenticated_select_municipal_tax_collection_runs"
  on public.municipal_tax_collection_runs for select to authenticated using (true);

drop policy if exists "authenticated_select_municipal_tax_debts" on public.municipal_tax_debts;
create policy "authenticated_select_municipal_tax_debts"
  on public.municipal_tax_debts for select to authenticated using (true);
