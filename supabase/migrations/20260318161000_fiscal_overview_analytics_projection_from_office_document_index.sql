-- Garante que a tabela exista (evita depender de ordem de migrações).
create table if not exists public.office_document_index (
  source text not null,
  source_record_id uuid not null,
  office_id uuid not null,
  company_id uuid not null,
  empresa text not null,
  cnpj text,
  category_key text not null,
  type text not null,
  origem text,
  status text,
  periodo text,
  document_date date,
  created_at timestamptz not null,
  file_path text,
  chave text,
  modelo text,
  tipo_certidao text,
  file_extension text,
  search_text_normalized text not null default '',
  updated_at timestamptz not null default now(),
  primary key (source, source_record_id)
);

-- Reescreve get_fiscal_overview_analytics_summary para usar office_document_index como fonte "docs".

create or replace function public.get_fiscal_overview_analytics_summary(
  company_ids uuid[] default null,
  date_from date default null,
  date_to date default null
)
returns jsonb
language sql
stable
security definer
set search_path = public
as $$
with params as (
  select
    public.current_office_id() as office_id,
    coalesce(company_ids, '{}'::uuid[]) as company_ids,
    coalesce(array_length(company_ids, 1), 0) as company_count,
    coalesce(least(date_from, date_to), current_date) as date_from,
    coalesce(greatest(date_from, date_to), current_date) as date_to,
    now()::date as today
),
docs as (
  select
    i.*,
    coalesce(i.document_date, i.created_at::date) as ref_date
  from public.office_document_index i
  join params p on p.office_id = i.office_id
  where i.source = 'fiscal'
    and (p.company_count = 0 or i.company_id = any (p.company_ids))
    and coalesce(i.document_date, i.created_at::date) between p.date_from and p.date_to
),
docs_by_type as (
  select upper(coalesce(type, 'OUTROS')) as name, count(*)::int as value
  from docs
  group by 1
),
docs_by_status as (
  select lower(coalesce(status::text, 'pendente')) as name, count(*)::int as value
  from docs
  group by 1
),
month_series as (
  select to_char(gs::date, 'YYYY-MM') as month_key
  from params p
  cross join generate_series(
    date_trunc('month', p.date_from)::date,
    date_trunc('month', p.date_to)::date,
    interval '1 month'
  ) as gs
),
docs_by_month as (
  select to_char(ref_date, 'YYYY-MM') as month_key, count(*)::int as value
  from docs
  group by 1
),
docs_by_company as (
  select
    d.company_id,
    coalesce(c.name, 'Empresa sem nome') as company_name,
    count(*)::int as value
  from docs d
  left join public.companies c
    on c.id = d.company_id
   and c.office_id = d.office_id
  group by d.company_id, c.name
  order by value desc, company_name asc
  limit 8
)
select jsonb_build_object(
  'cards',
  jsonb_build_object(
    'totalDocumentos', coalesce((select count(*)::int from docs), 0),
    'documentosHoje', coalesce((
      select count(*)::int
      from docs d
      join params p on true
      where d.ref_date = p.today
    ), 0),
    'documentosPendentes', coalesce((
      select count(*)::int
      from docs
      where lower(status::text) in ('pendente', 'processando', 'divergente')
    ), 0),
    'documentosRejeitados', coalesce((
      select count(*)::int
      from docs
      where lower(status::text) in ('rejeitado', 'rejected', 'cancelado', 'cancelada')
    ), 0),
    'empresasComEmissao', coalesce((select count(distinct company_id)::int from docs), 0)
  ),
  'byType',
  coalesce((
    select jsonb_agg(jsonb_build_object('name', name, 'value', value) order by value desc, name asc)
    from docs_by_type
  ), '[]'::jsonb),
  'byMonth',
  coalesce((
    select jsonb_agg(
      jsonb_build_object('key', ms.month_key, 'value', coalesce(dm.value, 0))
      order by ms.month_key
    )
    from month_series ms
    left join docs_by_month dm on dm.month_key = ms.month_key
  ), '[]'::jsonb),
  'byCompany',
  coalesce((
    select jsonb_agg(
      jsonb_build_object('name', company_name, 'value', value)
      order by value desc, company_name asc
    )
    from docs_by_company
  ), '[]'::jsonb),
  'byStatus',
  coalesce((
    select jsonb_agg(jsonb_build_object('name', name, 'value', value) order by value desc, name asc)
    from docs_by_status
  ), '[]'::jsonb),
  'byTypeSummary',
  jsonb_build_object(
    'NFS', coalesce((select value from docs_by_type where name = 'NFS' limit 1), 0),
    'NFE', coalesce((select value from docs_by_type where name = 'NFE' limit 1), 0),
    'NFC', coalesce((select value from docs_by_type where name = 'NFC' limit 1), 0),
    'outros', coalesce((select sum(value)::int from docs_by_type where name not in ('NFS', 'NFE', 'NFC')), 0)
  )
)
from params;
$$;

revoke all on function public.get_fiscal_overview_analytics_summary(uuid[], date, date) from public;
grant execute on function public.get_fiscal_overview_analytics_summary(uuid[], date, date) to authenticated;

