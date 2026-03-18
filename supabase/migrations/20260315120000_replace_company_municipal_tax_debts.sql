-- Substitui todos os débitos municipais de uma empresa pelos novos (delete + insert em uma transação).
-- O robô pode chamar esta RPC para garantir que a cada consulta os dados da empresa sejam substituídos, não acumulados.
create or replace function public.replace_company_municipal_tax_debts(
  p_company_id uuid,
  p_debts jsonb
)
returns integer
language plpgsql
security definer
set search_path = public
as $$
declare
  inserted_count int;
begin
  delete from public.municipal_tax_debts
  where company_id = p_company_id;

  if p_debts is null or jsonb_array_length(p_debts) = 0 then
    return 0;
  end if;

  insert into public.municipal_tax_debts (
    company_id,
    ano,
    tributo,
    numero_documento,
    data_vencimento,
    valor,
    situacao,
    portal_inscricao,
    portal_cai,
    detalhes,
    fetched_at
  )
  select
    p_company_id,
    nullif(trim(elem->>'ano'), '')::integer,
    coalesce(nullif(trim(elem->>'tributo'), ''), 'Tributo não identificado'),
    nullif(trim(elem->>'numero_documento'), ''),
    (nullif(trim(elem->>'data_vencimento'), ''))::date,
    coalesce((elem->>'valor')::numeric(14,2), 0),
    nullif(trim(elem->>'situacao'), ''),
    nullif(trim(elem->>'portal_inscricao'), ''),
    nullif(trim(elem->>'portal_cai'), ''),
    coalesce(elem->'detalhes', '{}'::jsonb),
    now()
  from jsonb_array_elements(p_debts) as elem;

  get diagnostics inserted_count = row_count;
  return inserted_count;
end;
$$;

comment on function public.replace_company_municipal_tax_debts(uuid, jsonb) is
  'Remove todos os débitos municipais da empresa e insere os novos. Usado pelo robô Taxas e Impostos para substituir (não acumular) os dados a cada execução.';

grant execute on function public.replace_company_municipal_tax_debts(uuid, jsonb) to anon;
grant execute on function public.replace_company_municipal_tax_debts(uuid, jsonb) to authenticated;
