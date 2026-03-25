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
        case when coalesce(sort_key, '') = 'data_vencimento' and p.sort_direction = 'asc' then data_vencimento end asc nulls last,
        case when coalesce(sort_key, '') = 'data_vencimento' and p.sort_direction = 'desc' then data_vencimento end desc nulls last,
        case when coalesce(sort_key, '') = 'valor' and p.sort_direction = 'asc' then valor end asc nulls last,
        case when coalesce(sort_key, '') = 'valor' and p.sort_direction = 'desc' then valor end desc nulls last,
        case when coalesce(sort_key, '') = 'situacao' and p.sort_direction = 'asc' then situacao end asc,
        case when coalesce(sort_key, '') = 'situacao' and p.sort_direction = 'desc' then situacao end desc,
        case when coalesce(sort_key, '') = 'status_class' and p.sort_direction = 'asc' then status_class end asc,
        case when coalesce(sort_key, '') = 'status_class' and p.sort_direction = 'desc' then status_class end desc,
        data_vencimento asc nulls last,
        company_name asc,
        id asc
    ) as row_num
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
where row_num > (p.page_number - 1) * p.page_size
  and row_num <= p.page_number * p.page_size
order by row_num;
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
by_status as (
  select jsonb_agg(
    jsonb_build_object(
      'key', status_key,
      'name', status_name,
      'total', total_count
    )
    order by sort_order
  ) as rows
  from (
    values
      ('vencido', 'Vencido', (select count(*)::int from filtered where status_class = 'vencido'), 1),
      ('a_vencer', 'A vencer (proximos 30 dias)', (select count(*)::int from filtered where status_class = 'a_vencer'), 2),
      ('regular', 'Regular', (select count(*)::int from filtered where status_class = 'regular'), 3)
  ) as x(status_key, status_name, total_count, sort_order)
),
due_soon as (
  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'id', id,
        'company_id', company_id,
        'company_name', company_name,
        'company_document', company_document,
        'ano', ano,
        'tributo', tributo,
        'numero_documento', numero_documento,
        'data_vencimento', data_vencimento,
        'valor', valor,
        'situacao', situacao,
        'status_class', status_class,
        'days_until_due', days_until_due,
        'guia_pdf_path', guia_pdf_path
      )
      order by data_vencimento asc nulls last, company_name asc
    ),
    '[]'::jsonb
  ) as rows
  from (
    select *
    from filtered
    where data_vencimento is not null
      and data_vencimento >= current_date
    order by data_vencimento asc nulls last, company_name asc
    limit 30
  ) due
),
by_company as (
  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'name', company_name,
        'total', total_valor
      )
      order by total_valor desc, company_name asc
    ),
    '[]'::jsonb
  ) as rows
  from (
    select company_name, coalesce(sum(valor), 0)::numeric as total_valor
    from filtered
    group by company_name
    order by total_valor desc, company_name asc
    limit 8
  ) ranked
),
by_year as (
  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'name', ano::text,
        'total', total_valor
      )
      order by ano asc
    ),
    '[]'::jsonb
  ) as rows
  from (
    select ano, coalesce(sum(valor), 0)::numeric as total_valor
    from filtered
    where ano is not null
    group by ano
    order by ano asc
  ) years
),
years as (
  select coalesce(jsonb_agg(ano order by ano desc), '[]'::jsonb) as rows
  from (
    select distinct ano
    from filtered
    where ano is not null
    order by ano desc
  ) distinct_years
)
select jsonb_build_object(
  'cards',
  jsonb_build_object(
    'totalDebitos', coalesce((select quantidade_debitos from summary), 0),
    'totalVencido', coalesce((select total_vencido from summary), 0),
    'totalAVencer', coalesce((select total_a_vencer from summary), 0),
    'quantidadeDebitos', coalesce((select quantidade_debitos from summary), 0),
    'empresasComVencidos', coalesce((select empresas_com_vencidos from summary), 0),
    'empresasProximasVencimento', coalesce((select empresas_proximas_vencimento from summary), 0),
    'totalValor', coalesce((select total_valor from summary), 0)
  ),
  'byStatus', coalesce((select rows from by_status), '[]'::jsonb),
  'dueSoon', coalesce((select rows from due_soon), '[]'::jsonb),
  'byCompany', coalesce((select rows from by_company), '[]'::jsonb),
  'byYear', coalesce((select rows from by_year), '[]'::jsonb),
  'years', coalesce((select rows from years), '[]'::jsonb)
);
$$;
