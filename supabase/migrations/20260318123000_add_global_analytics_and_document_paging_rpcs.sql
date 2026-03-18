create or replace function public.try_parse_jsonb(value text)
returns jsonb
language plpgsql
immutable
as $$
begin
  return coalesce(nullif(btrim(value), '')::jsonb, '{}'::jsonb);
exception when others then
  return '{}'::jsonb;
end;
$$;

create or replace function public.get_document_rows_page(
  company_ids uuid[] default null,
  category_filter text default null,
  file_kind text default null,
  search_text text default null,
  date_from date default null,
  date_to date default null,
  page_number integer default 1,
  page_size integer default 25
)
returns table (
  id uuid,
  company_id uuid,
  empresa text,
  cnpj text,
  source text,
  category_key text,
  type text,
  origem text,
  status text,
  periodo text,
  document_date date,
  created_at timestamptz,
  file_path text,
  chave text,
  total_count integer
)
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
    lower(coalesce(nullif(btrim(category_filter), ''), 'todos')) as category_filter,
    lower(coalesce(nullif(btrim(file_kind), ''), 'todos')) as file_kind,
    lower(btrim(coalesce(search_text, ''))) as search_text,
    least(date_from, date_to) as date_from,
    greatest(date_from, date_to) as date_to,
    greatest(coalesce(page_number, 1), 1) as page_number,
    least(greatest(coalesce(page_size, 25), 1), 200) as page_size
),
fiscal_rows as (
  select
    fd.id,
    fd.company_id,
    c.name as empresa,
    c.document as cnpj,
    'fiscal'::text as source,
    case
      when fd.type = 'NFS' then 'nfs'
      when fd.type in ('NFE', 'NFC') then 'nfe_nfc'
      else 'fiscal_outros'
    end as category_key,
    fd.type,
    case
      when fd.file_path ilike '%/Recebidas/%' then 'recebidas'
      when fd.file_path ilike '%/Emitidas/%' then 'emitidas'
      else null
    end as origem,
    fd.status::text as status,
    fd.periodo,
    coalesce(fd.document_date, fd.created_at::date) as document_date,
    fd.created_at,
    fd.file_path,
    fd.chave
  from public.fiscal_documents fd
  join params p on p.office_id = fd.office_id
  join public.companies c
    on c.id = fd.company_id
   and c.office_id = fd.office_id
  where p.company_count = 0 or fd.company_id = any (p.company_ids)
),
certs_raw as (
  select
    se.id,
    se.company_id,
    se.created_at,
    public.try_parse_jsonb(se.payload) as payload
  from public.sync_events se
  join params p on p.office_id = se.office_id
  where se.tipo = 'certidao_resultado'
    and (p.company_count = 0 or se.company_id = any (p.company_ids))
),
certs_latest as (
  select distinct on (company_id, coalesce(payload ->> 'tipo_certidao', ''))
    id,
    company_id,
    created_at,
    payload
  from certs_raw
  where coalesce(payload ->> 'tipo_certidao', '') <> ''
  order by company_id, coalesce(payload ->> 'tipo_certidao', ''), created_at desc
),
cert_rows as (
  select
    cl.id,
    cl.company_id,
    c.name as empresa,
    c.document as cnpj,
    'certidoes'::text as source,
    'certidoes'::text as category_key,
    ('CERTIDÃO - ' ||
      case lower(coalesce(cl.payload ->> 'tipo_certidao', ''))
        when 'federal' then 'Federal'
        when 'fgts' then 'FGTS'
        when 'estadual_go' then 'Estadual (GO)'
        else coalesce(cl.payload ->> 'tipo_certidao', 'Outra')
      end
    ) as type,
    null::text as origem,
    case
      when lower(coalesce(cl.payload ->> 'status', '')) in ('regular', 'negativa') then 'negativa'
      when lower(coalesce(cl.payload ->> 'status', '')) = 'positiva' then 'positiva'
      else 'irregular'
    end as status,
    nullif(btrim(coalesce(cl.payload ->> 'periodo', '')), '') as periodo,
    coalesce(nullif(cl.payload ->> 'document_date', ''), nullif(cl.payload ->> 'data_consulta', ''))::date as document_date,
    cl.created_at,
    nullif(btrim(coalesce(cl.payload ->> 'arquivo_pdf', '')), '') as file_path,
    null::text as chave
  from certs_latest cl
  join params p on true
  join public.companies c
    on c.id = cl.company_id
   and c.office_id = p.office_id
),
dp_rows as (
  select
    g.id,
    g.company_id,
    c.name as empresa,
    c.document as cnpj,
    'dp_guias'::text as source,
    'taxas_impostos'::text as category_key,
    'GUIA - ' || coalesce(g.tipo, 'OUTROS') as type,
    null::text as origem,
    null::text as status,
    left(coalesce(g.data::text, ''), 7) as periodo,
    g.data::date as document_date,
    g.created_at,
    g.file_path,
    null::text as chave
  from public.dp_guias g
  join params p on p.office_id = g.office_id
  join public.companies c
    on c.id = g.company_id
   and c.office_id = g.office_id
  where p.company_count = 0 or g.company_id = any (p.company_ids)
),
municipal_rows as (
  select
    m.id,
    m.company_id,
    c.name as empresa,
    c.document as cnpj,
    'municipal_taxes'::text as source,
    'taxas_impostos'::text as category_key,
    'IMPOSTO/TAXA - ' || coalesce(m.tributo, 'OUTROS') as type,
    null::text as origem,
    case
      when coalesce(m.valor, 0) = 0 then 'regular'
      when m.data_vencimento is null then 'regular'
      when m.data_vencimento < current_date then 'vencido'
      when m.data_vencimento <= current_date + 30 then 'a_vencer'
      else 'regular'
    end as status,
    left(coalesce(m.data_vencimento::text, ''), 7) as periodo,
    m.data_vencimento as document_date,
    coalesce(m.fetched_at, m.created_at) as created_at,
    m.guia_pdf_path as file_path,
    null::text as chave
  from public.municipal_tax_debts m
  join params p on p.office_id = m.office_id
  join public.companies c
    on c.id = m.company_id
   and c.office_id = m.office_id
  where p.company_count = 0 or m.company_id = any (p.company_ids)
),
rows_union as (
  select * from fiscal_rows
  union all
  select * from cert_rows
  union all
  select * from dp_rows
  union all
  select * from municipal_rows
),
filtered as (
  select *
  from rows_union r
  join params p on true
  where coalesce(btrim(r.file_path), '') <> ''
    and (p.category_filter = 'todos' or r.category_key = p.category_filter)
    and (
      p.file_kind = 'todos'
      or (p.file_kind = 'xml' and lower(r.file_path) like '%.xml')
      or (p.file_kind = 'pdf' and lower(r.file_path) like '%.pdf')
    )
    and (p.date_from is null or coalesce(r.document_date, r.created_at::date) >= p.date_from)
    and (p.date_to is null or coalesce(r.document_date, r.created_at::date) <= p.date_to)
    and (
      p.search_text = ''
      or lower(concat_ws(' ', r.empresa, r.cnpj, r.type, r.status, r.periodo, r.file_path, r.chave, r.origem)) like '%' || p.search_text || '%'
      or public.only_digits(coalesce(r.cnpj, '')) like '%' || public.only_digits(p.search_text) || '%'
      or coalesce(r.chave, '') like '%' || btrim(coalesce(search_text, '')) || '%'
    )
),
ordered as (
  select
    f.*,
    count(*) over ()::int as total_count,
    row_number() over (
      order by coalesce(f.document_date, f.created_at::date) desc, f.created_at desc, f.id desc
    )::int as row_num
  from filtered f
)
select
  id,
  company_id,
  empresa,
  cnpj,
  source,
  category_key,
  type,
  origem,
  status,
  periodo,
  document_date,
  created_at,
  file_path,
  chave,
  total_count
from ordered
join params p on true
where ordered.row_num > (p.page_number - 1) * p.page_size
  and ordered.row_num <= p.page_number * p.page_size
order by ordered.row_num;
$$;

create or replace function public.get_fiscal_detail_documents_page(
  detail_kind text,
  company_ids uuid[] default null,
  search_text text default null,
  date_from date default null,
  date_to date default null,
  file_kind text default null,
  origem_filter text default null,
  modelo_filter text default null,
  certidao_tipo_filter text default null,
  page_number integer default 1,
  page_size integer default 25
)
returns table (
  id uuid,
  company_id uuid,
  empresa text,
  cnpj text,
  type text,
  chave text,
  periodo text,
  status text,
  document_date date,
  created_at timestamptz,
  file_path text,
  origem text,
  modelo text,
  tipo_certidao text,
  total_count integer
)
language sql
stable
security definer
set search_path = public
as $$
with params as (
  select
    public.current_office_id() as office_id,
    upper(coalesce(detail_kind, 'NFS')) as detail_kind,
    coalesce(company_ids, '{}'::uuid[]) as company_ids,
    coalesce(array_length(company_ids, 1), 0) as company_count,
    lower(btrim(coalesce(search_text, ''))) as search_text,
    least(date_from, date_to) as date_from,
    greatest(date_from, date_to) as date_to,
    lower(coalesce(nullif(btrim(file_kind), ''), 'all')) as file_kind,
    lower(coalesce(nullif(btrim(origem_filter), ''), 'all')) as origem_filter,
    lower(coalesce(nullif(btrim(modelo_filter), ''), 'all')) as modelo_filter,
    lower(coalesce(nullif(btrim(certidao_tipo_filter), ''), 'all')) as certidao_tipo_filter,
    greatest(coalesce(page_number, 1), 1) as page_number,
    least(greatest(coalesce(page_size, 25), 1), 200) as page_size
),
fiscal_rows as (
  select
    fd.id,
    fd.company_id,
    c.name as empresa,
    c.document as cnpj,
    fd.type,
    fd.chave,
    fd.periodo,
    fd.status::text as status,
    coalesce(fd.document_date, fd.created_at::date) as document_date,
    fd.created_at,
    fd.file_path,
    case
      when fd.file_path ilike '%/Recebidas/%' then 'recebidas'
      when fd.file_path ilike '%/Emitidas/%' then 'emitidas'
      else null
    end as origem,
    case
      when fd.type = 'NFE' then '55'
      when fd.type = 'NFC' then '65'
      else null
    end as modelo,
    null::text as tipo_certidao
  from public.fiscal_documents fd
  join params p on p.office_id = fd.office_id
  join public.companies c
    on c.id = fd.company_id
   and c.office_id = fd.office_id
  where p.detail_kind in ('NFS', 'NFE', 'NFC', 'NFE_NFC')
    and (
      (p.detail_kind = 'NFS' and fd.type = 'NFS')
      or (p.detail_kind = 'NFE' and fd.type = 'NFE')
      or (p.detail_kind = 'NFC' and fd.type = 'NFC')
      or (p.detail_kind = 'NFE_NFC' and fd.type in ('NFE', 'NFC'))
    )
    and (p.company_count = 0 or fd.company_id = any (p.company_ids))
),
certs_raw as (
  select
    se.id,
    se.company_id,
    se.created_at,
    public.try_parse_jsonb(se.payload) as payload
  from public.sync_events se
  join params p on p.office_id = se.office_id
  where p.detail_kind = 'CERTIDOES'
    and se.tipo = 'certidao_resultado'
    and (p.company_count = 0 or se.company_id = any (p.company_ids))
),
certs_latest as (
  select distinct on (company_id, coalesce(payload ->> 'tipo_certidao', ''))
    id,
    company_id,
    created_at,
    payload
  from certs_raw
  where coalesce(payload ->> 'tipo_certidao', '') <> ''
  order by company_id, coalesce(payload ->> 'tipo_certidao', ''), created_at desc
),
cert_rows as (
  select
    cl.id,
    cl.company_id,
    c.name as empresa,
    c.document as cnpj,
    ('CERTIDÃO - ' ||
      case lower(coalesce(cl.payload ->> 'tipo_certidao', ''))
        when 'federal' then 'Federal'
        when 'fgts' then 'FGTS'
        when 'estadual_go' then 'Estadual (GO)'
        else coalesce(cl.payload ->> 'tipo_certidao', 'Outra')
      end
    ) as type,
    null::text as chave,
    nullif(btrim(coalesce(cl.payload ->> 'periodo', '')), '') as periodo,
    case
      when lower(coalesce(cl.payload ->> 'status', '')) in ('regular', 'negativa') then 'negativa'
      when lower(coalesce(cl.payload ->> 'status', '')) = 'positiva' then 'positiva'
      else 'irregular'
    end as status,
    coalesce(nullif(cl.payload ->> 'document_date', ''), nullif(cl.payload ->> 'data_consulta', ''))::date as document_date,
    cl.created_at,
    nullif(btrim(coalesce(cl.payload ->> 'arquivo_pdf', '')), '') as file_path,
    null::text as origem,
    null::text as modelo,
    lower(coalesce(cl.payload ->> 'tipo_certidao', '')) as tipo_certidao
  from certs_latest cl
  join params p on true
  join public.companies c
    on c.id = cl.company_id
   and c.office_id = p.office_id
),
rows_union as (
  select * from fiscal_rows
  union all
  select * from cert_rows
),
filtered as (
  select *
  from rows_union r
  join params p on true
  where coalesce(btrim(r.file_path), '') <> ''
    and (p.date_from is null or coalesce(r.document_date, r.created_at::date) >= p.date_from)
    and (p.date_to is null or coalesce(r.document_date, r.created_at::date) <= p.date_to)
    and (
      p.search_text = ''
      or lower(concat_ws(' ', r.empresa, r.cnpj, r.type, r.chave, r.status, r.tipo_certidao)) like '%' || p.search_text || '%'
      or public.only_digits(coalesce(r.cnpj, '')) like '%' || public.only_digits(p.search_text) || '%'
      or coalesce(r.chave, '') like '%' || btrim(coalesce(search_text, '')) || '%'
    )
    and (
      p.file_kind = 'all'
      or (p.file_kind = 'xml' and lower(r.file_path) like '%.xml')
      or (p.file_kind = 'pdf' and lower(r.file_path) like '%.pdf')
    )
    and (
      p.origem_filter = 'all'
      or p.detail_kind <> 'NFS'
      or coalesce(r.origem, '') = p.origem_filter
    )
    and (
      p.modelo_filter = 'all'
      or p.detail_kind <> 'NFE_NFC'
      or coalesce(r.modelo, '') = p.modelo_filter
    )
    and (
      p.certidao_tipo_filter = 'all'
      or p.detail_kind <> 'CERTIDOES'
      or coalesce(r.tipo_certidao, '') = p.certidao_tipo_filter
    )
),
ordered as (
  select
    f.*,
    count(*) over ()::int as total_count,
    row_number() over (
      order by coalesce(f.document_date, f.created_at::date) desc, f.created_at desc, f.id desc
    )::int as row_num
  from filtered f
)
select
  id,
  company_id,
  empresa,
  cnpj,
  type,
  chave,
  periodo,
  status,
  document_date,
  created_at,
  file_path,
  origem,
  modelo,
  tipo_certidao,
  total_count
from ordered
join params p on true
where ordered.row_num > (p.page_number - 1) * p.page_size
  and ordered.row_num <= p.page_number * p.page_size
order by ordered.row_num;
$$;

create or replace function public.get_certidoes_overview_summary(company_ids uuid[] default null)
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
    coalesce(array_length(company_ids, 1), 0) as company_count
),
certs_raw as (
  select
    se.id,
    se.company_id,
    se.created_at,
    public.try_parse_jsonb(se.payload) as payload
  from public.sync_events se
  join params p on p.office_id = se.office_id
  where se.tipo = 'certidao_resultado'
    and (p.company_count = 0 or se.company_id = any (p.company_ids))
),
certs_latest as (
  select distinct on (company_id, coalesce(payload ->> 'tipo_certidao', ''))
    id,
    company_id,
    created_at,
    payload
  from certs_raw
  where coalesce(payload ->> 'tipo_certidao', '') <> ''
  order by company_id, coalesce(payload ->> 'tipo_certidao', ''), created_at desc
),
normalized as (
  select
    id,
    company_id,
    case
      when lower(coalesce(payload ->> 'status', '')) in ('regular', 'negativa') then 'negativa'
      when lower(coalesce(payload ->> 'status', '')) = 'positiva' then 'positiva'
      else 'irregular'
    end as status
  from certs_latest
),
summary as (
  select
    count(*)::int as total,
    count(*) filter (where status = 'negativa')::int as negativas,
    count(*) filter (where status <> 'negativa')::int as irregulares
  from normalized
)
select jsonb_build_object(
  'cards',
  jsonb_build_object(
    'total', coalesce((select total from summary), 0),
    'negativas', coalesce((select negativas from summary), 0),
    'irregulares', coalesce((select irregulares from summary), 0)
  ),
  'chartData',
  jsonb_build_array(
    jsonb_build_object('name', 'Negativas', 'value', coalesce((select negativas from summary), 0)),
    jsonb_build_object('name', 'Irregulares', 'value', coalesce((select irregulares from summary), 0))
  )
);
$$;

create or replace function public.get_fiscal_detail_summary(
  detail_kind text,
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
    upper(coalesce(detail_kind, 'NFS')) as detail_kind,
    coalesce(company_ids, '{}'::uuid[]) as company_ids,
    coalesce(array_length(company_ids, 1), 0) as company_count,
    least(date_from, date_to) as date_from,
    greatest(date_from, date_to) as date_to,
    current_date as today,
    to_char(current_date, 'YYYY-MM') as current_month
),
docs as (
  select
    d.id,
    d.company_id,
    d.type,
    d.periodo,
    d.status,
    d.file_path,
    d.chave,
    coalesce(d.document_date, d.created_at::date) as ref_date
  from public.fiscal_documents d
  join params p on p.office_id = d.office_id
  where (
      (p.detail_kind = 'NFS' and d.type = 'NFS')
      or (p.detail_kind = 'NFE' and d.type = 'NFE')
      or (p.detail_kind = 'NFC' and d.type = 'NFC')
      or (p.detail_kind = 'NFE_NFC' and d.type in ('NFE', 'NFC'))
    )
    and (p.company_count = 0 or d.company_id = any (p.company_ids))
    and (p.date_from is null or coalesce(d.document_date, d.created_at::date) >= p.date_from)
    and (p.date_to is null or coalesce(d.document_date, d.created_at::date) <= p.date_to)
),
month_bounds as (
  select
    coalesce(date_trunc('month', (select date_from from params))::date, date_trunc('month', current_date)::date - interval '11 month') as start_month,
    coalesce(date_trunc('month', (select date_to from params))::date, date_trunc('month', current_date)::date) as end_month
),
month_series as (
  select to_char(gs::date, 'YYYY-MM') as month_key
  from month_bounds b
  cross join generate_series(b.start_month, b.end_month, interval '1 month') as gs
),
docs_by_month as (
  select to_char(ref_date, 'YYYY-MM') as month_key, count(distinct coalesce(chave, id::text))::int as value
  from docs
  group by 1
),
summary as (
  select
    count(distinct coalesce(chave, id::text))::int as total_documents,
    count(*) filter (where coalesce(file_path, '') <> '')::int as available_documents,
    count(distinct coalesce(chave, id::text)) filter (where left(coalesce(periodo, ''), 7) = (select current_month from params))::int as this_month,
    count(*) filter (where type = 'NFE')::int as nfe_count,
    count(*) filter (where type = 'NFC')::int as nfc_count
  from docs
)
select jsonb_build_object(
  'cards',
  jsonb_build_object(
    'totalDocuments', coalesce((select total_documents from summary), 0),
    'availableDocuments', coalesce((select available_documents from summary), 0),
    'thisMonth', coalesce((select this_month from summary), 0),
    'nfeCount', coalesce((select nfe_count from summary), 0),
    'nfcCount', coalesce((select nfc_count from summary), 0)
  ),
  'byMonth',
  coalesce((
    select jsonb_agg(
      jsonb_build_object('key', ms.month_key, 'value', coalesce(dm.value, 0))
      order by ms.month_key
    )
    from month_series ms
    left join docs_by_month dm on dm.month_key = ms.month_key
  ), '[]'::jsonb)
);
$$;

create or replace function public.get_ir_overview_summary(responsavel_filter text default null)
returns jsonb
language sql
stable
security definer
set search_path = public
as $$
with params as (
  select
    public.current_office_id() as office_id,
    nullif(btrim(responsavel_filter), '') as responsavel_filter
),
rows as (
  select *
  from public.ir_clients c
  join params p on p.office_id = c.office_id
  where p.responsavel_filter is null or coalesce(c.responsavel_ir, '') = p.responsavel_filter
),
summary as (
  select
    count(*)::int as total,
    count(*) filter (where status_pagamento <> 'A PAGAR')::int as paid_count,
    count(*) filter (where status_pagamento = 'A PAGAR')::int as pending_count,
    count(*) filter (where status_declaracao = 'Concluido')::int as concluded_count,
    count(*) filter (where status_declaracao <> 'Concluido')::int as pending_execution_count,
    coalesce(sum(valor_servico), 0)::numeric as total_value,
    coalesce(sum(valor_servico) filter (where status_pagamento <> 'A PAGAR'), 0)::numeric as paid_value,
    coalesce(sum(valor_servico) filter (where status_pagamento = 'A PAGAR'), 0)::numeric as pending_value
  from rows
)
select jsonb_build_object(
  'cards',
  jsonb_build_object(
    'clientesIr', coalesce((select total from summary), 0),
    'recebidos', coalesce((select paid_count from summary), 0),
    'aPagar', coalesce((select pending_count from summary), 0),
    'concluidoPercent',
      case
        when coalesce((select total from summary), 0) = 0 then 0
        else round((coalesce((select concluded_count from summary), 0)::numeric / (select total from summary)::numeric) * 100)
      end,
    'concluidoTotal', coalesce((select concluded_count from summary), 0),
    'clientesTotal', coalesce((select total from summary), 0),
    'valorTotal', coalesce((select total_value from summary), 0)
  ),
  'progressData',
  jsonb_build_array(
    jsonb_build_object('name', 'Concluídos', 'value', coalesce((select concluded_count from summary), 0)),
    jsonb_build_object('name', 'Pendentes', 'value', coalesce((select pending_execution_count from summary), 0))
  ),
  'paymentValueData',
  jsonb_build_array(
    jsonb_build_object('name', 'Recebido', 'value', coalesce((select paid_value from summary), 0)),
    jsonb_build_object('name', 'A PAGAR', 'value', coalesce((select pending_value from summary), 0))
  ),
  'paidValuePercent',
    case
      when coalesce((select total_value from summary), 0) = 0 then 0
      else round((coalesce((select paid_value from summary), 0) / (select total_value from summary)) * 100)
    end
);
$$;

create or replace function public.get_operations_overview_summary()
returns jsonb
language sql
stable
security definer
set search_path = public
as $$
with params as (
  select public.current_office_id() as office_id
),
completed as (
  select *
  from public.execution_requests e
  join params p on p.office_id = e.office_id
  where e.status in ('completed', 'failed')
),
summary as (
  select
    count(*) filter (
      where coalesce(completed_at, created_at) >= date_trunc('day', now())
    )::int as eventos_hoje,
    count(*) filter (
      where coalesce(completed_at, created_at) >= date_trunc('day', now()) - interval '1 day'
        and coalesce(completed_at, created_at) < date_trunc('day', now())
    )::int as eventos_ontem,
    count(*) filter (where status = 'completed')::int as success_count,
    count(*) filter (where status = 'failed')::int as fail_count,
    count(*)::int as total_count
  from completed
),
robot_summary as (
  select count(*)::int as robots_count
  from public.robots
)
select jsonb_build_object(
  'eventosHoje', coalesce((select eventos_hoje from summary), 0),
  'eventosOntem', coalesce((select eventos_ontem from summary), 0),
  'falhas', coalesce((select fail_count from summary), 0),
  'robots', coalesce((select robots_count from robot_summary), 0),
  'taxaSucesso',
    case
      when coalesce((select total_count from summary), 0) = 0 then 0
      else round((coalesce((select success_count from summary), 0)::numeric / (select total_count from summary)::numeric) * 1000) / 10
    end
);
$$;

create or replace function public.get_nfs_stats_range_summary(
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
    to_char(coalesce(least(date_from, date_to), current_date), 'YYYY-MM') as month_from,
    to_char(coalesce(greatest(date_from, date_to), current_date), 'YYYY-MM') as month_to,
    to_char((date_trunc('month', coalesce(least(date_from, date_to), current_date)) - interval '1 month')::date, 'YYYY-MM') as prev_month
),
rows as (
  select *
  from public.nfs_stats n
  join params p on p.office_id = n.office_id
  where n.period between p.month_from and p.month_to
    and (p.company_count = 0 or n.company_id = any (p.company_ids))
),
prev_rows as (
  select *
  from public.nfs_stats n
  join params p on p.office_id = n.office_id
  where n.period = p.prev_month
    and (p.company_count = 0 or n.company_id = any (p.company_ids))
),
current_summary as (
  select
    coalesce(sum(qty_emitidas), 0)::numeric as qty_emitidas,
    coalesce(sum(qty_recebidas), 0)::numeric as qty_recebidas,
    coalesce(sum(valor_emitidas), 0)::numeric as valor_emitidas,
    coalesce(sum(valor_recebidas), 0)::numeric as valor_recebidas
  from rows
),
prev_summary as (
  select
    coalesce(sum(valor_emitidas), 0)::numeric as valor_emitidas,
    coalesce(sum(valor_recebidas), 0)::numeric as valor_recebidas
  from prev_rows
),
emitidas_codes as (
  select
    coalesce(item ->> 'code', item ->> 'codigo', item ->> 'service_code', item ->> 'ctribnac', item ->> 'cTribNac', '') as code,
    coalesce(item ->> 'description', item ->> 'descricao', item ->> 'label', item ->> 'name', item ->> 'xTribNac', item ->> 'xtribnac', '') as description,
    sum(
      coalesce(
        nullif(item ->> 'total_value', '')::numeric,
        nullif(item ->> 'totalValue', '')::numeric,
        nullif(item ->> 'valor_total', '')::numeric,
        nullif(item ->> 'valor', '')::numeric,
        nullif(item ->> 'value', '')::numeric,
        nullif(item ->> 'amount', '')::numeric,
        nullif(item ->> 'total', '')::numeric,
        0
      )
    ) as total_value
  from rows r
  cross join lateral jsonb_array_elements(
    case
      when jsonb_typeof(r.service_codes_emitidas) = 'array' then r.service_codes_emitidas
      when jsonb_typeof(r.service_codes_emitidas -> 'service_codes') = 'array' then r.service_codes_emitidas -> 'service_codes'
      else '[]'::jsonb
    end
  ) item
  group by 1, 2
),
recebidas_codes as (
  select
    coalesce(item ->> 'code', item ->> 'codigo', item ->> 'service_code', item ->> 'ctribnac', item ->> 'cTribNac', '') as code,
    coalesce(item ->> 'description', item ->> 'descricao', item ->> 'label', item ->> 'name', item ->> 'xTribNac', item ->> 'xtribnac', '') as description,
    sum(
      coalesce(
        nullif(item ->> 'total_value', '')::numeric,
        nullif(item ->> 'totalValue', '')::numeric,
        nullif(item ->> 'valor_total', '')::numeric,
        nullif(item ->> 'valor', '')::numeric,
        nullif(item ->> 'value', '')::numeric,
        nullif(item ->> 'amount', '')::numeric,
        nullif(item ->> 'total', '')::numeric,
        0
      )
    ) as total_value
  from rows r
  cross join lateral jsonb_array_elements(
    case
      when jsonb_typeof(r.service_codes_recebidas) = 'array' then r.service_codes_recebidas
      when jsonb_typeof(r.service_codes_recebidas -> 'service_codes') = 'array' then r.service_codes_recebidas -> 'service_codes'
      else '[]'::jsonb
    end
  ) item
  group by 1, 2
)
select jsonb_build_object(
  'period', (select month_from from params) || case when (select month_from from params) <> (select month_to from params) then ' a ' || (select month_to from params) else '' end,
  'totalQty', coalesce((select qty_emitidas + qty_recebidas from current_summary), 0),
  'valorEmitidas', coalesce((select valor_emitidas from current_summary), 0),
  'valorRecebidas', coalesce((select valor_recebidas from current_summary), 0),
  'previousValorEmitidas', coalesce((select valor_emitidas from prev_summary), 0),
  'previousValorRecebidas', coalesce((select valor_recebidas from prev_summary), 0),
  'serviceCodesRankingPrestadas',
  coalesce((
    select jsonb_agg(jsonb_build_object('code', code, 'description', description, 'total_value', total_value) order by total_value desc, code asc)
    from emitidas_codes
  ), '[]'::jsonb),
  'serviceCodesRankingTomadas',
  coalesce((
    select jsonb_agg(jsonb_build_object('code', code, 'description', description, 'total_value', total_value) order by total_value desc, code asc)
    from recebidas_codes
  ), '[]'::jsonb)
);
$$;

create or replace function public.get_municipal_tax_debts_page(
  company_ids uuid[] default null,
  year_filter text default null,
  status_filter text default null,
  date_from date default null,
  date_to date default null,
  search_text text default null,
  sort_key text default null,
  sort_direction text default 'desc',
  page_number integer default 1,
  page_size integer default 25
)
returns table (
  id uuid,
  company_id uuid,
  company_name text,
  company_document text,
  ano integer,
  tributo text,
  numero_documento text,
  data_vencimento date,
  valor numeric,
  situacao text,
  status_class text,
  days_until_due integer,
  guia_pdf_path text,
  total_count integer
)
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
    nullif(btrim(year_filter), '') as year_filter,
    nullif(lower(btrim(status_filter)), '') as status_filter,
    least(date_from, date_to) as date_from,
    greatest(date_from, date_to) as date_to,
    lower(btrim(coalesce(search_text, ''))) as search_text,
    case
      when lower(coalesce(sort_direction, 'desc')) = 'asc' then 'asc'
      else 'desc'
    end as sort_direction,
    greatest(coalesce(page_number, 1), 1) as page_number,
    least(greatest(coalesce(page_size, 25), 1), 200) as page_size
),
base as (
  select
    d.id,
    d.company_id,
    c.name as company_name,
    c.document as company_document,
    d.ano,
    d.tributo,
    d.numero_documento,
    d.data_vencimento,
    d.valor,
    d.situacao,
    d.guia_pdf_path,
    case
      when coalesce(d.valor, 0) = 0 then 'regular'
      when d.data_vencimento is null then 'regular'
      when d.data_vencimento < current_date then 'vencido'
      when d.data_vencimento <= current_date + 30 then 'a_vencer'
      else 'regular'
    end as status_class,
    case
      when d.data_vencimento is null then null
      else (d.data_vencimento - current_date)::int
    end as days_until_due
  from public.municipal_tax_debts d
  join params p on p.office_id = d.office_id
  join public.companies c
    on c.id = d.company_id
   and c.office_id = d.office_id
  where (p.company_count = 0 or d.company_id = any (p.company_ids))
),
filtered as (
  select *
  from base b
  join params p on true
  where (p.year_filter is null or p.year_filter = 'todos' or coalesce(b.ano::text, '') = p.year_filter)
    and (p.status_filter is null or p.status_filter = 'todos' or b.status_class = p.status_filter)
    and (p.date_from is null or coalesce(b.data_vencimento, p.date_from) >= p.date_from)
    and (p.date_to is null or coalesce(b.data_vencimento, p.date_to) <= p.date_to)
    and (
      p.search_text = ''
      or lower(concat_ws(' ', b.company_name, b.company_document, b.tributo, b.numero_documento, b.situacao)) like '%' || p.search_text || '%'
      or public.only_digits(coalesce(b.company_document, '')) like '%' || public.only_digits(p.search_text) || '%'
    )
),
ordered as (
  select
    f.*,
    count(*) over ()::int as total_count,
    row_number() over (
      order by
        case when coalesce(sort_key, '') = 'company_name' and p.sort_direction = 'asc' then company_name end asc,
        case when coalesce(sort_key, '') = 'company_name' and p.sort_direction = 'desc' then company_name end desc,
        case when coalesce(sort_key, '') = 'tributo' and p.sort_direction = 'asc' then tributo end asc,
        case when coalesce(sort_key, '') = 'tributo' and p.sort_direction = 'desc' then tributo end desc,
        case when coalesce(sort_key, '') = 'ano' and p.sort_direction = 'asc' then ano end asc,
        case when coalesce(sort_key, '') = 'ano' and p.sort_direction = 'desc' then ano end desc,
        case when coalesce(sort_key, '') = 'numero_documento' and p.sort_direction = 'asc' then numero_documento end asc,
        case when coalesce(sort_key, '') = 'numero_documento' and p.sort_direction = 'desc' then numero_documento end desc,
        case when coalesce(sort_key, '') = 'data_vencimento' and p.sort_direction = 'asc' then data_vencimento end asc,
        case when coalesce(sort_key, '') = 'data_vencimento' and p.sort_direction = 'desc' then data_vencimento end desc,
        case when coalesce(sort_key, '') = 'valor' and p.sort_direction = 'asc' then valor end asc,
        case when coalesce(sort_key, '') = 'valor' and p.sort_direction = 'desc' then valor end desc,
        case when coalesce(sort_key, '') = 'situacao' and p.sort_direction = 'asc' then situacao end asc,
        case when coalesce(sort_key, '') = 'situacao' and p.sort_direction = 'desc' then situacao end desc,
        case when coalesce(sort_key, '') = 'status_class' and p.sort_direction = 'asc' then status_class end asc,
        case when coalesce(sort_key, '') = 'status_class' and p.sort_direction = 'desc' then status_class end desc,
        data_vencimento asc nulls last,
        company_name asc,
        id desc
    )::int as row_num
  from filtered f
  join params p on true
)
select
  id,
  company_id,
  company_name,
  company_document,
  ano,
  tributo,
  numero_documento,
  data_vencimento,
  valor,
  situacao,
  status_class,
  days_until_due,
  guia_pdf_path,
  total_count
from ordered
join params p on true
where ordered.row_num > (p.page_number - 1) * p.page_size
  and ordered.row_num <= p.page_number * p.page_size
order by ordered.row_num;
$$;

create or replace function public.get_tax_intelligence_overview_summary(company_ids uuid[] default null)
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
    coalesce(array_length(company_ids, 1), 0) as company_count
),
rows as (
  select
    c.id,
    c.company_id,
    c.updated_at,
    c.result_payload,
    coalesce(c.result_payload ->> 'appliedAnnex', '') as applied_annex,
    coalesce(c.result_payload ->> 'apurationPeriod', '') as apuration_period,
    coalesce(nullif(c.result_payload ->> 'estimatedDas', ''), '0')::numeric as estimated_das,
    coalesce(nullif(c.result_payload ->> 'effectiveRate', ''), '0')::numeric as effective_rate
  from public.simple_national_calculations c
  join params p on p.office_id = c.office_id
  where p.company_count = 0 or c.company_id = any (p.company_ids)
),
summary as (
  select
    count(*)::int as total_rows,
    count(distinct company_id)::int as empresas_ativas,
    coalesce(avg(estimated_das), 0)::numeric as media_das,
    coalesce(avg(effective_rate), 0)::numeric as media_aliquota
  from rows
),
month_counts as (
  select apuration_period as month_key, count(*)::int as value
  from rows
  where apuration_period ~ '^\d{4}-\d{2}$'
  group by 1
  order by 1 desc
  limit 6
),
annex_counts as (
  select applied_annex as annex, count(*)::int as value
  from rows
  where applied_annex <> ''
  group by 1
  order by 1
),
recent as (
  select
    r.id,
    r.company_id,
    coalesce(comp.name, 'Empresa') as company_name,
    r.apuration_period,
    r.applied_annex,
    r.estimated_das,
    r.updated_at
  from rows r
  left join public.companies comp
    on comp.id = r.company_id
   and comp.office_id = (select office_id from params)
  order by r.updated_at desc
  limit 5
)
select jsonb_build_object(
  'cards',
  jsonb_build_object(
    'calculosSalvos', coalesce((select total_rows from summary), 0),
    'mediaDas', coalesce((select media_das from summary), 0),
    'mediaAliquotaEfetiva', coalesce((select media_aliquota from summary), 0),
    'empresasAtivas', coalesce((select empresas_ativas from summary), 0)
  ),
  'byTopic',
  jsonb_build_array(
    jsonb_build_object('name', 'Visão Geral', 'value', 1),
    jsonb_build_object('name', 'Simples Nacional', 'value', greatest(coalesce((select total_rows from summary), 0), 1)),
    jsonb_build_object('name', 'Lucro Real', 'value', 1),
    jsonb_build_object('name', 'Lucro Presumido', 'value', 1)
  ),
  'byMonth',
  coalesce((
    select jsonb_agg(
      jsonb_build_object('name', to_char(to_date(month_key || '-01', 'YYYY-MM-DD'), 'Mon/YY'), 'value', value)
      order by month_key
    )
    from (select * from month_counts order by month_key asc) ordered_months
  ), '[]'::jsonb),
  'annexDistribution',
  coalesce((
    select jsonb_agg(jsonb_build_object('name', 'Anexo ' || annex, 'value', value) order by annex)
    from annex_counts
  ), '[]'::jsonb),
  'recentCalculations',
  coalesce((
    select jsonb_agg(
      jsonb_build_object(
        'id', id,
        'companyName', company_name,
        'apurationPeriod', apuration_period,
        'appliedAnnex', applied_annex,
        'estimatedDas', estimated_das,
        'updatedAt', updated_at
      )
      order by updated_at desc
    )
    from recent
  ), '[]'::jsonb)
);
$$;

create or replace function public.get_paralegal_certificate_overview_summary(company_ids uuid[] default null)
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
    coalesce(array_length(company_ids, 1), 0) as company_count
),
items as (
  select
    c.id,
    case
      when coalesce(c.cert_blob_b64, '') <> '' and c.auth_mode = 'certificate' then true
      else false
    end as has_certificate,
    case
      when coalesce(c.cert_blob_b64, '') <> '' and c.auth_mode = 'certificate' and c.cert_valid_until is not null
        then (c.cert_valid_until::date - current_date)::int
      else null
    end as days_to_expiry,
    case
      when not (coalesce(c.cert_blob_b64, '') <> '' and c.auth_mode = 'certificate') then 'sem_certificado'
      when c.cert_valid_until is null then 'vencido'
      when c.cert_valid_until::date < current_date then 'vencido'
      when c.cert_valid_until::date <= current_date + 30 then 'vence_em_breve'
      else 'ativo'
    end as certificate_status
  from public.companies c
  join params p on p.office_id = c.office_id
  where p.company_count = 0 or c.id = any (p.company_ids)
),
summary as (
  select
    count(*)::int as total,
    count(*) filter (where certificate_status = 'ativo')::int as ativos,
    count(*) filter (where certificate_status = 'vence_em_breve')::int as vence_em_breve,
    count(*) filter (where certificate_status = 'vencido')::int as vencidos,
    count(*) filter (where certificate_status = 'sem_certificado')::int as sem_certificado
  from items
)
select jsonb_build_object(
  'cards',
  jsonb_build_object(
    'total', coalesce((select total from summary), 0),
    'ativos', coalesce((select ativos from summary), 0),
    'venceEmBreve', coalesce((select vence_em_breve from summary), 0),
    'vencidos', coalesce((select vencidos from summary), 0),
    'semCertificado', coalesce((select sem_certificado from summary), 0)
  ),
  'byStatus',
  jsonb_build_array(
    jsonb_build_object('key', 'ativo', 'name', 'Ativos', 'total', coalesce((select ativos from summary), 0)),
    jsonb_build_object('key', 'vence_em_breve', 'name', 'Perto de vencer', 'total', coalesce((select vence_em_breve from summary), 0)),
    jsonb_build_object('key', 'vencido', 'name', 'Vencidos', 'total', coalesce((select vencidos from summary), 0)),
    jsonb_build_object('key', 'sem_certificado', 'name', 'Sem certificado', 'total', coalesce((select sem_certificado from summary), 0))
  )
);
$$;

create or replace function public.get_municipal_tax_overview_summary(
  company_ids uuid[] default null,
  year_filter text default null,
  status_filter text default null,
  date_from date default null,
  date_to date default null,
  search_text text default null
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
    nullif(btrim(year_filter), '') as year_filter,
    nullif(lower(btrim(status_filter)), '') as status_filter,
    least(date_from, date_to) as date_from,
    greatest(date_from, date_to) as date_to,
    lower(btrim(coalesce(search_text, ''))) as search_text
),
base as (
  select
    d.id,
    d.company_id,
    c.name as company_name,
    c.document as company_document,
    d.ano,
    d.tributo,
    d.numero_documento,
    d.data_vencimento,
    d.valor,
    d.situacao,
    d.guia_pdf_path,
    case
      when coalesce(d.valor, 0) = 0 then 'regular'
      when d.data_vencimento is null then 'regular'
      when d.data_vencimento < current_date then 'vencido'
      when d.data_vencimento <= current_date + 30 then 'a_vencer'
      else 'regular'
    end as status_class,
    case
      when d.data_vencimento is null then null
      else (d.data_vencimento - current_date)::int
    end as days_until_due
  from public.municipal_tax_debts d
  join params p on p.office_id = d.office_id
  join public.companies c
    on c.id = d.company_id
   and c.office_id = d.office_id
  where (p.company_count = 0 or d.company_id = any (p.company_ids))
),
filtered as (
  select *
  from base b
  join params p on true
  where (p.year_filter is null or p.year_filter = 'todos' or coalesce(b.ano::text, '') = p.year_filter)
    and (p.status_filter is null or p.status_filter = 'todos' or b.status_class = p.status_filter)
    and (p.date_from is null or coalesce(b.data_vencimento, p.date_from) >= p.date_from)
    and (p.date_to is null or coalesce(b.data_vencimento, p.date_to) <= p.date_to)
    and (
      p.search_text = ''
      or lower(concat_ws(' ', b.company_name, b.company_document, b.tributo, b.numero_documento, b.situacao)) like '%' || p.search_text || '%'
      or public.only_digits(coalesce(b.company_document, '')) like '%' || public.only_digits(p.search_text) || '%'
    )
),
summary as (
  select
    count(*)::int as quantidade_debitos,
    count(distinct company_id) filter (where status_class = 'vencido')::int as empresas_com_vencidos,
    count(distinct company_id) filter (where status_class = 'a_vencer')::int as empresas_proximas_vencimento,
    coalesce(sum(valor), 0)::numeric as total_valor,
    coalesce(sum(valor) filter (where status_class = 'vencido'), 0)::numeric as total_vencido,
    coalesce(sum(valor) filter (where status_class = 'a_vencer'), 0)::numeric as total_a_vencer
  from filtered
),
status_counts as (
  select status_class as key, count(*)::int as total
  from filtered
  group by 1
),
due_soon as (
  select *
  from filtered
  where data_vencimento is not null
    and data_vencimento >= current_date
  order by data_vencimento asc, company_name asc
  limit 30
),
company_totals as (
  select company_name as name, coalesce(sum(valor), 0)::numeric as total
  from filtered
  group by 1
  order by total desc, name asc
  limit 8
),
year_totals as (
  select coalesce(ano, 0)::text as name, coalesce(sum(valor), 0)::numeric as total
  from filtered
  group by 1
  order by name asc
),
years as (
  select distinct ano
  from filtered
  where ano is not null
  order by ano desc
)
select jsonb_build_object(
  'cards',
  jsonb_build_object(
    'totalValor', coalesce((select total_valor from summary), 0),
    'totalVencido', coalesce((select total_vencido from summary), 0),
    'totalAVencer', coalesce((select total_a_vencer from summary), 0),
    'quantidadeDebitos', coalesce((select quantidade_debitos from summary), 0),
    'empresasComVencidos', coalesce((select empresas_com_vencidos from summary), 0),
    'empresasProximasVencimento', coalesce((select empresas_proximas_vencimento from summary), 0)
  ),
  'byStatus',
  jsonb_build_array(
    jsonb_build_object('key', 'vencido', 'name', 'Vencido', 'total', coalesce((select total from status_counts where key = 'vencido'), 0)),
    jsonb_build_object('key', 'a_vencer', 'name', 'A vencer (proximos 30 dias)', 'total', coalesce((select total from status_counts where key = 'a_vencer'), 0)),
    jsonb_build_object('key', 'regular', 'name', 'Regular', 'total', coalesce((select total from status_counts where key = 'regular'), 0))
  ),
  'dueSoon',
  coalesce((
    select jsonb_agg(
      jsonb_build_object(
        'id', id,
        'company_id', company_id,
        'company_name', company_name,
        'company_document', company_document,
        'tributo', tributo,
        'data_vencimento', data_vencimento,
        'valor', valor,
        'status_class', status_class,
        'days_until_due', days_until_due,
        'guia_pdf_path', guia_pdf_path
      )
      order by data_vencimento asc, company_name asc
    )
    from due_soon
  ), '[]'::jsonb),
  'byCompany',
  coalesce((
    select jsonb_agg(jsonb_build_object('name', name, 'total', total) order by total desc, name asc)
    from company_totals
  ), '[]'::jsonb),
  'byYear',
  coalesce((
    select jsonb_agg(jsonb_build_object('name', name, 'total', total) order by name asc)
    from year_totals
  ), '[]'::jsonb),
  'years',
  coalesce((select jsonb_agg(ano order by ano desc) from years), '[]'::jsonb)
);
$$;
