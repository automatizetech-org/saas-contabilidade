create or replace function public.get_dashboard_overview_summary(company_ids uuid[] default null)
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
    now()::date as today,
    to_char(now(), 'YYYY-MM') as current_month
),
docs as (
  select
    d.*,
    coalesce(d.document_date, d.created_at::date) as ref_date
  from public.fiscal_documents d
  join params p on p.office_id = d.office_id
  where p.company_count = 0 or d.company_id = any (p.company_ids)
),
docs_by_type as (
  select
    case
      when type = 'NFS' then 'NFS-e'
      when type = 'NFE' then 'NF-e'
      when type = 'NFC' then 'NFC-e'
      else type
    end as name,
    count(*)::int as value
  from docs
  group by 1
),
docs_status as (
  select coalesce(nullif(status::text, ''), 'pendente') as name, count(*)::int as value
  from docs
  group by 1
),
month_series as (
  select to_char(gs::date, 'YYYY-MM') as month_key
  from generate_series(
    date_trunc('month', now())::date - interval '5 month',
    date_trunc('month', now())::date,
    interval '1 month'
  ) as gs
),
docs_by_month as (
  select to_char(ref_date, 'YYYY-MM') as month_key, count(*)::int as value
  from docs
  group by 1
),
top_companies as (
  select
    d.company_id,
    coalesce(c.name, 'Empresa sem nome') as company_name,
    count(*)::int as total
  from docs d
  left join public.companies c
    on c.id = d.company_id
   and c.office_id = d.office_id
  group by d.company_id, c.name
  order by total desc, company_name asc
  limit 6
),
fiscal_pendencias_abertas as (
  select fp.*
  from public.fiscal_pendencias fp
  join params p on p.office_id = fp.office_id
  where (p.company_count = 0 or fp.company_id = any (p.company_ids))
    and lower(fp.status::text) <> 'concluido'
),
dp_checklist_rows as (
  select dc.*
  from public.dp_checklist dc
  join params p on p.office_id = dc.office_id
  where p.company_count = 0 or dc.company_id = any (p.company_ids)
),
dp_pendencias as (
  select *
  from dp_checklist_rows
  where lower(status::text) <> 'concluido'
),
dp_guias_rows as (
  select dg.*
  from public.dp_guias dg
  join params p on p.office_id = dg.office_id
  where p.company_count = 0 or dg.company_id = any (p.company_ids)
),
dp_guias_por_tipo as (
  select coalesce(nullif(tipo, ''), 'Outros') as name, count(*)::int as value
  from dp_guias_rows
  group by 1
),
financial_rows as (
  select fr.*
  from public.financial_records fr
  join params p on p.office_id = fr.office_id
  where p.company_count = 0 or fr.company_id = any (p.company_ids)
),
financial_month_series as (
  select to_char(gs::date, 'YYYY-MM') as month_key
  from generate_series(
    date_trunc('month', now())::date - interval '5 month',
    date_trunc('month', now())::date,
    interval '1 month'
  ) as gs
),
financial_by_month as (
  select periodo as month_key, count(*)::int as value
  from financial_rows
  where periodo ~ '^\d{4}-\d{2}$'
  group by 1
),
sync_recent as (
  select
    se.id,
    se.company_id,
    se.tipo,
    se.status,
    se.created_at,
    coalesce(c.name, 'Sistema') as company_name
  from public.sync_events se
  join params p on p.office_id = se.office_id
  left join public.companies c
    on c.id = se.company_id
   and c.office_id = se.office_id
  where p.company_count = 0 or se.company_id is null or se.company_id = any (p.company_ids)
  order by se.created_at desc
  limit 8
)
select jsonb_build_object(
  'companiesCount',
  coalesce((
    select count(*)::int
    from public.companies c
    join params p on p.office_id = c.office_id
  ), 0),
  'documentsCount',
  coalesce((select count(*)::int from docs), 0),
  'importedDocuments',
  coalesce((select count(*)::int from docs where coalesce(file_path, '') <> ''), 0),
  'totalDocuments',
  coalesce((select count(*)::int from docs), 0),
  'docsByType',
  coalesce((
    select jsonb_agg(jsonb_build_object('name', name, 'value', value) order by value desc, name asc)
    from docs_by_type
  ), '[]'::jsonb),
  'documentsPerMonth',
  coalesce((
    select jsonb_agg(
      jsonb_build_object('key', ms.month_key, 'value', coalesce(dm.value, 0))
      order by ms.month_key
    )
    from month_series ms
    left join docs_by_month dm on dm.month_key = ms.month_key
  ), '[]'::jsonb),
  'processingStatus',
  coalesce((
    select jsonb_agg(jsonb_build_object('name', name, 'value', value) order by value desc, name asc)
    from docs_status
  ), '[]'::jsonb),
  'topCompanies',
  coalesce((
    select jsonb_agg(
      jsonb_build_object('companyId', company_id, 'companyName', company_name, 'total', total)
      order by total desc, company_name asc
    )
    from top_companies
  ), '[]'::jsonb),
  'fiscalSummary',
  jsonb_build_object(
    'totalPendencias', coalesce((select count(*)::int from fiscal_pendencias_abertas), 0),
    'totalDocumentos', coalesce((select count(*)::int from docs), 0)
  ),
  'dpSummary',
  jsonb_build_object(
    'totalPendencias', coalesce((select count(*)::int from dp_pendencias), 0),
    'totalChecklist', coalesce((select count(*)::int from dp_checklist_rows), 0),
    'totalGuias', coalesce((select count(*)::int from dp_guias_rows), 0),
    'folhaProcessadaMes', coalesce((
      select count(*)::int
      from dp_checklist_rows dc
      join params p on true
      where lower(dc.tarefa) like '%folha%'
        and lower(dc.status::text) = 'concluido'
        and left(coalesce(dc.competencia, ''), 7) = p.current_month
    ), 0),
    'empresasAtivas', coalesce((
      select count(*)::int
      from (
        select company_id from dp_guias_rows
        union
        select company_id from dp_checklist_rows
      ) companies
    ), 0),
    'guiasPorTipo', coalesce((
      select jsonb_agg(jsonb_build_object('name', name, 'value', value) order by value desc, name asc)
      from dp_guias_por_tipo
    ), '[]'::jsonb)
  ),
  'contabilSummary',
  jsonb_build_object(
    'balancosGerados', coalesce((select count(*)::int from financial_rows), 0),
    'empresasAtualizadas', coalesce((
      select count(distinct company_id)::int
      from financial_rows
      where lower(status::text) in ('concluido', 'completed', 'validado', 'atualizado', 'pago')
    ), 0),
    'empresasPendentes', coalesce((
      select count(distinct company_id)::int
      from financial_rows
      where lower(status::text) not in ('concluido', 'completed', 'validado', 'atualizado', 'pago')
         or coalesce(pendencias_count, 0) > 0
    ), 0),
    'lancamentosNoPeriodo', coalesce((
      select count(*)::int
      from financial_rows fr
      join params p on true
      where left(coalesce(fr.periodo, ''), 7) = p.current_month
    ), 0),
    'lancamentosPorMes', coalesce((
      select jsonb_agg(
        jsonb_build_object('key', ms.month_key, 'value', coalesce(fm.value, 0))
        order by ms.month_key
      )
      from financial_month_series ms
      left join financial_by_month fm on fm.month_key = ms.month_key
    ), '[]'::jsonb)
  ),
  'pendingTabs',
  jsonb_build_object(
    'fiscal', coalesce((select count(*)::int from fiscal_pendencias_abertas), 0),
    'dp', coalesce((select count(*)::int from dp_pendencias), 0),
    'total', coalesce((select count(*)::int from fiscal_pendencias_abertas), 0) + coalesce((select count(*)::int from dp_pendencias), 0)
  ),
  'executiveSummary',
  jsonb_build_object(
    'fiscal', jsonb_build_object(
      'totalDocumentos', coalesce((select count(*)::int from docs), 0),
      'processadosHoje', coalesce((
        select count(*)::int
        from docs d
        join params p on true
        where d.ref_date = p.today
      ), 0),
      'empresasAtivas', coalesce((select count(distinct company_id)::int from docs), 0)
    ),
    'dp', jsonb_build_object(
      'guiasGeradas', coalesce((select count(*)::int from dp_guias_rows), 0),
      'guiasPendentes', coalesce((select count(*)::int from dp_pendencias), 0),
      'folhaProcessadaMes', coalesce((
        select count(*)::int
        from dp_checklist_rows dc
        join params p on true
        where lower(dc.tarefa) like '%folha%'
          and lower(dc.status::text) = 'concluido'
          and left(coalesce(dc.competencia, ''), 7) = p.current_month
      ), 0)
    ),
    'contabil', jsonb_build_object(
      'balancosGerados', coalesce((select count(*)::int from financial_rows), 0),
      'empresasAtualizadas', coalesce((
        select count(distinct company_id)::int
        from financial_rows
        where lower(status::text) in ('concluido', 'completed', 'validado', 'atualizado', 'pago')
      ), 0),
      'pendentes', coalesce((
        select count(distinct company_id)::int
        from financial_rows
        where lower(status::text) not in ('concluido', 'completed', 'validado', 'atualizado', 'pago')
           or coalesce(pendencias_count, 0) > 0
      ), 0)
    )
  ),
  'syncSummary',
  jsonb_build_object(
    'totalEventos', coalesce((select count(*)::int from sync_recent), 0),
    'falhas', coalesce((select count(*)::int from sync_recent where lower(status::text) = 'failed'), 0),
    'sucessos', coalesce((select count(*)::int from sync_recent where lower(status::text) = 'completed'), 0)
  ),
  'syncEvents',
  coalesce((
    select jsonb_agg(
      jsonb_build_object(
        'id', id,
        'company_id', company_id,
        'tipo', tipo,
        'status', status,
        'created_at', created_at,
        'companyName', company_name
      )
      order by created_at desc
    )
    from sync_recent
  ), '[]'::jsonb)
)
from params;
$$;

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
    d.*,
    coalesce(d.document_date, d.created_at::date) as ref_date
  from public.fiscal_documents d
  join params p on p.office_id = d.office_id
  where (p.company_count = 0 or d.company_id = any (p.company_ids))
    and coalesce(d.document_date, d.created_at::date) between p.date_from and p.date_to
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
