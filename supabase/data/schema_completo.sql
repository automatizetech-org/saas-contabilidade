-- =============================================================================
-- SCHEMA COMPLETO SUPABASE - BOOTSTRAP OFICIAL
-- Multi-tenant por escritorio (office) para ambiente novo.
-- =============================================================================

create extension if not exists pgcrypto with schema extensions;

do $$
begin
  create type public.document_status as enum ('novo', 'pendente', 'concluido', 'processando', 'enviado', 'erro', 'baixado');
exception when duplicate_object then null;
end $$;

do $$
begin
  create type public.platform_role as enum ('super_admin', 'user');
exception when duplicate_object then null;
end $$;

do $$
begin
  create type public.office_role as enum ('owner', 'admin', 'operator', 'viewer');
exception when duplicate_object then null;
end $$;

do $$
begin
  create type public.office_status as enum ('draft', 'active', 'inactive');
exception when duplicate_object then null;
end $$;

do $$
begin
  create type public.server_status as enum ('pending', 'online', 'offline', 'error', 'disabled');
exception when duplicate_object then null;
end $$;

do $$
begin
  create type public.robot_job_status as enum ('pending', 'claimed', 'running', 'completed', 'failed', 'cancelled', 'timed_out');
exception when duplicate_object then null;
end $$;

create or replace function public.set_updated_at()
returns trigger
language plpgsql
as $$
begin
  new.updated_at = now();
  return new;
end;
$$;

create or replace function public.only_digits(value text)
returns text
language sql
immutable
as $$
  select regexp_replace(coalesce(value, ''), '\D', '', 'g')
$$;

create or replace function public.is_valid_cpf(value text)
returns boolean
language plpgsql
immutable
as $$
declare
  digits text := public.only_digits(value);
  sum_value integer := 0;
  remainder_value integer;
  idx integer;
begin
  if length(digits) <> 11 or digits ~ '^(\d)\1{10}$' then
    return false;
  end if;

  for idx in 1..9 loop
    sum_value := sum_value + cast(substr(digits, idx, 1) as integer) * (11 - idx);
  end loop;

  remainder_value := (sum_value * 10) % 11;
  if remainder_value = 10 then remainder_value := 0; end if;
  if remainder_value <> cast(substr(digits, 10, 1) as integer) then
    return false;
  end if;

  sum_value := 0;
  for idx in 1..10 loop
    sum_value := sum_value + cast(substr(digits, idx, 1) as integer) * (12 - idx);
  end loop;

  remainder_value := (sum_value * 10) % 11;
  if remainder_value = 10 then remainder_value := 0; end if;
  return remainder_value = cast(substr(digits, 11, 1) as integer);
end;
$$;

create or replace function public.is_valid_cnpj(value text)
returns boolean
language plpgsql
immutable
as $$
declare
  digits text := public.only_digits(value);
  sum_value integer := 0;
  remainder_value integer;
  idx integer;
  weights_one integer[] := array[5,4,3,2,9,8,7,6,5,4,3,2];
  weights_two integer[] := array[6,5,4,3,2,9,8,7,6,5,4,3,2];
begin
  if length(digits) <> 14 or digits ~ '^(\d)\1{13}$' then
    return false;
  end if;

  for idx in 1..12 loop
    sum_value := sum_value + cast(substr(digits, idx, 1) as integer) * weights_one[idx];
  end loop;

  remainder_value := sum_value % 11;
  if remainder_value < 2 then remainder_value := 0; else remainder_value := 11 - remainder_value; end if;
  if remainder_value <> cast(substr(digits, 13, 1) as integer) then
    return false;
  end if;

  sum_value := 0;
  for idx in 1..13 loop
    sum_value := sum_value + cast(substr(digits, idx, 1) as integer) * weights_two[idx];
  end loop;

  remainder_value := sum_value % 11;
  if remainder_value < 2 then remainder_value := 0; else remainder_value := 11 - remainder_value; end if;
  return remainder_value = cast(substr(digits, 14, 1) as integer);
end;
$$;

create or replace function public.is_valid_cpf_or_cnpj(value text)
returns boolean
language sql
immutable
as $$
  select case
    when length(public.only_digits(value)) = 11 then public.is_valid_cpf(value)
    when length(public.only_digits(value)) = 14 then public.is_valid_cnpj(value)
    else false
  end
$$;

create or replace function public.are_valid_portal_logins(value jsonb)
returns boolean
language plpgsql
immutable
as $$
declare
  item jsonb;
  cpf_value text;
  password_value text;
  default_count integer := 0;
  seen_cpfs text[] := array[]::text[];
begin
  if value is null then
    return true;
  end if;

  if jsonb_typeof(value) <> 'array' then
    return false;
  end if;

  for item in select jsonb_array_elements(value)
  loop
    if jsonb_typeof(item) <> 'object' then
      return false;
    end if;

    cpf_value := public.only_digits(item ->> 'cpf');
    password_value := btrim(coalesce(item ->> 'password', item ->> 'senha', ''));

    if not public.is_valid_cpf(cpf_value) or password_value = '' then
      return false;
    end if;

    if cpf_value = any(seen_cpfs) then
      return false;
    end if;

    seen_cpfs := array_append(seen_cpfs, cpf_value);

    if lower(coalesce(item ->> 'is_default', 'false')) = 'true' then
      default_count := default_count + 1;
    end if;
  end loop;

  return default_count <= 1;
exception when others then
  return false;
end;
$$;

create or replace function public.normalize_tax_document_fields()
returns trigger
language plpgsql
as $$
begin
  if tg_table_name = 'accountants' then
    new.cpf := public.only_digits(new.cpf);
  elsif tg_table_name = 'companies' then
    new.document := nullif(public.only_digits(new.document), '');
    new.contador_cpf := nullif(public.only_digits(new.contador_cpf), '');
  elsif tg_table_name = 'company_robot_config' then
    new.selected_login_cpf := nullif(public.only_digits(new.selected_login_cpf), '');
  elsif tg_table_name = 'ir_clients' then
    new.cpf_cnpj := public.only_digits(new.cpf_cnpj);
  end if;

  return new;
end;
$$;

create or replace function public.prevent_duplicate_company_document()
returns trigger
language plpgsql
as $$
begin
  if coalesce(new.document, '') = '' then
    return new;
  end if;

  if tg_op = 'INSERT'
     or public.only_digits(new.document) is distinct from public.only_digits(old.document)
     or new.office_id is distinct from old.office_id then
    if exists (
      select 1
      from public.companies c
      where c.office_id = new.office_id
        and c.id <> coalesce(new.id, '00000000-0000-0000-0000-000000000000'::uuid)
        and public.only_digits(c.document) = public.only_digits(new.document)
    ) then
      raise exception 'Já existe uma empresa cadastrada com este CNPJ neste escritório.';
    end if;
  end if;

  return new;
end;
$$;

create table if not exists public.profiles (
  id uuid primary key references auth.users(id) on delete cascade,
  username text not null,
  role public.platform_role not null default 'user',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create unique index if not exists profiles_username_key on public.profiles (lower(username));

create table if not exists public.offices (
  id uuid primary key default gen_random_uuid(),
  name text not null,
  slug text not null,
  status public.office_status not null default 'draft',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create unique index if not exists offices_slug_key on public.offices (lower(slug));

create table if not exists public.office_memberships (
  id uuid primary key default gen_random_uuid(),
  office_id uuid not null references public.offices(id) on delete cascade,
  user_id uuid not null references public.profiles(id) on delete cascade,
  role public.office_role not null default 'viewer',
  panel_access jsonb not null default '{
    "dashboard": true,
    "fiscal": true,
    "dp": true,
    "contabil": true,
    "inteligencia_tributaria": true,
    "ir": true,
    "paralegal": false,
    "financeiro": true,
    "operacoes": true,
    "documentos": true,
    "empresas": true,
    "alteracao_empresarial": false,
    "sync": false
  }'::jsonb,
  is_default boolean not null default true,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint office_memberships_user_id_key unique (user_id),
  constraint office_memberships_office_user_key unique (office_id, user_id)
);

create unique index if not exists office_memberships_default_key
  on public.office_memberships (user_id)
  where is_default = true;

create or replace function public.current_platform_role()
returns public.platform_role
language sql
stable
security definer
set search_path = public
as $$
  select role from public.profiles where id = auth.uid()
$$;

create or replace function public.is_platform_super_admin()
returns boolean
language sql
stable
security definer
set search_path = public
as $$
  select coalesce(public.current_platform_role() = 'super_admin', false)
$$;

create or replace function public.current_office_id()
returns uuid
language sql
stable
security definer
set search_path = public
as $$
  select office_id
  from public.office_memberships
  where user_id = auth.uid()
  order by is_default desc, created_at asc
  limit 1
$$;

create or replace function public.can_view_office(target_office_id uuid)
returns boolean
language sql
stable
security definer
set search_path = public
as $$
  select public.is_platform_super_admin()
    or exists (
      select 1
      from public.office_memberships om
      where om.user_id = auth.uid()
        and om.office_id = target_office_id
    )
$$;

create or replace function public.can_manage_office(target_office_id uuid)
returns boolean
language sql
stable
security definer
set search_path = public
as $$
  select public.is_platform_super_admin()
    or exists (
      select 1
      from public.office_memberships om
      where om.user_id = auth.uid()
        and om.office_id = target_office_id
        and om.role in ('owner')
    )
$$;

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

create or replace function public.guard_profile_role_update()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
begin
  if auth.uid() is null then
    return new;
  end if;

  if auth.uid() = new.id
     and not public.is_platform_super_admin()
     and new.role is distinct from old.role then
    raise exception 'Apenas super_admin pode alterar o papel de plataforma.';
  end if;

  return new;
end;
$$;

create or replace function public.handle_new_user_profile()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
declare
  v_name text;
begin
  v_name := nullif(trim(new.raw_user_meta_data ->> 'display_name'), '');
  if v_name is null then
    v_name := nullif(trim(new.raw_user_meta_data ->> 'full_name'), '');
  end if;
  if v_name is null then
    v_name := split_part(coalesce(new.email, ''), '@', 1);
  end if;

  insert into public.profiles (id, username, role)
  values (new.id, coalesce(v_name, 'usuario'), 'user')
  on conflict (id) do update
    set username = excluded.username,
        updated_at = now();

  return new;
end;
$$;

drop trigger if exists on_auth_user_created on auth.users;
create trigger on_auth_user_created
  after insert on auth.users
  for each row execute procedure public.handle_new_user_profile();

create table if not exists public.office_servers (
  id uuid primary key default gen_random_uuid(),
  office_id uuid not null references public.offices(id) on delete cascade,
  public_base_url text not null,
  base_path text not null default '',
  status public.server_status not null default 'pending',
  is_active boolean not null default true,
  connector_version text,
  min_supported_connector_version text,
  last_seen_at timestamptz,
  last_job_at timestamptz,
  host_fingerprint text,
  base_path_fingerprint text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create unique index if not exists office_servers_one_active_per_office on public.office_servers (office_id) where is_active = true;

create table if not exists public.office_server_credentials (
  id uuid primary key default gen_random_uuid(),
  office_server_id uuid not null unique references public.office_servers(id) on delete cascade,
  secret_hash text not null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists public.office_branding (
  id uuid primary key default gen_random_uuid(),
  office_id uuid not null unique references public.offices(id) on delete cascade,
  display_name text,
  primary_color text,
  secondary_color text,
  accent_color text,
  logo_path text,
  favicon_path text,
  use_custom_palette boolean not null default false,
  use_custom_logo boolean not null default false,
  use_custom_favicon boolean not null default false,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists public.office_audit_logs (
  id uuid primary key default gen_random_uuid(),
  office_id uuid references public.offices(id) on delete cascade,
  actor_user_id uuid references public.profiles(id) on delete set null,
  action text not null,
  entity_type text not null,
  entity_id text,
  payload jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create table if not exists public.robots (
  id uuid primary key default gen_random_uuid(),
  technical_id text not null unique,
  display_name text not null,
  status text not null default 'inactive' check (status in ('active', 'inactive', 'processing')),
  last_heartbeat_at timestamptz,
  segment_path text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  notes_mode text,
  date_execution_mode text,
  initial_period_start date,
  initial_period_end date,
  last_period_end date,
  is_fiscal_notes_robot boolean not null default false,
  fiscal_notes_kind text,
  global_logins jsonb not null default '[]'::jsonb
);

create table if not exists public.folder_structure_templates (
  id uuid primary key default gen_random_uuid(),
  parent_id uuid references public.folder_structure_templates(id) on delete cascade,
  name text not null,
  slug text,
  date_rule text check (date_rule in ('year', 'year_month', 'year_month_day')),
  position integer not null default 0,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists public.folder_structure_nodes (
  id uuid primary key default gen_random_uuid(),
  office_id uuid not null default public.current_office_id() references public.offices(id) on delete cascade,
  parent_id uuid references public.folder_structure_nodes(id) on delete cascade,
  name text not null,
  slug text,
  date_rule text check (date_rule in ('year', 'year_month', 'year_month_day')),
  position integer not null default 0,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists public.admin_settings (
  id uuid primary key default gen_random_uuid(),
  office_id uuid not null default public.current_office_id() references public.offices(id) on delete cascade,
  key text not null,
  value text not null default '',
  updated_at timestamptz not null default now(),
  constraint admin_settings_office_key_key unique (office_id, key)
);

create table if not exists public.accountants (
  id uuid primary key default gen_random_uuid(),
  office_id uuid not null default public.current_office_id() references public.offices(id) on delete cascade,
  name text not null,
  cpf text not null,
  active boolean not null default true,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint accountants_office_cpf_key unique (office_id, cpf)
);

create table if not exists public.companies (
  id uuid primary key default gen_random_uuid(),
  office_id uuid not null default public.current_office_id() references public.offices(id) on delete cascade,
  name text not null,
  document text,
  active boolean not null default true,
  created_by uuid references public.profiles(id),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  auth_mode text check (auth_mode is null or auth_mode in ('password', 'certificate')),
  cert_blob_b64 text,
  cert_password text,
  cert_valid_until date,
  contador_nome text,
  contador_cpf text,
  state_registration text,
  sefaz_go_logins jsonb not null default '[]'::jsonb,
  state_code text,
  city_name text,
  cae text
);

create index if not exists companies_office_name_idx on public.companies (office_id, name);

create table if not exists public.company_robot_config (
  id uuid primary key default gen_random_uuid(),
  office_id uuid not null default public.current_office_id() references public.offices(id) on delete cascade,
  company_id uuid not null references public.companies(id) on delete cascade,
  robot_technical_id text not null references public.robots(technical_id) on delete cascade,
  enabled boolean not null default true,
  auth_mode text not null default 'password' check (auth_mode in ('password', 'certificate')),
  nfs_password text,
  selected_login_cpf text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint company_robot_config_company_robot_key unique (company_id, robot_technical_id)
);

create table if not exists public.schedule_rules (
  id uuid primary key default gen_random_uuid(),
  office_id uuid not null default public.current_office_id() references public.offices(id) on delete cascade,
  company_ids uuid[] not null default '{}',
  robot_technical_ids text[] not null default '{}',
  notes_mode text,
  period_start date,
  period_end date,
  run_at_time time not null,
  run_daily boolean not null default true,
  status text not null default 'active' check (status in ('active', 'paused', 'completed')),
  last_run_at timestamptz,
  created_by uuid references public.profiles(id),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  run_at_date date,
  execution_mode text not null default 'sequential'
);

create table if not exists public.execution_requests (
  id uuid primary key default gen_random_uuid(),
  office_id uuid not null default public.current_office_id() references public.offices(id) on delete cascade,
  company_ids uuid[] not null default '{}',
  robot_technical_ids text[] not null default '{}',
  status text not null default 'pending' check (status in ('pending', 'running', 'completed', 'failed')),
  robot_id uuid references public.robots(id),
  claimed_at timestamptz,
  completed_at timestamptz,
  error_message text,
  period_start date,
  period_end date,
  created_at timestamptz not null default now(),
  created_by uuid references public.profiles(id),
  notes_mode text,
  schedule_rule_id uuid references public.schedule_rules(id) on delete set null,
  execution_mode text not null default 'sequential',
  execution_group_id uuid,
  execution_order integer
);

create table if not exists public.robot_display_config (
  office_id uuid not null default public.current_office_id() references public.offices(id) on delete cascade,
  robot_technical_id text not null references public.robots(technical_id) on delete cascade,
  company_ids uuid[] not null default '{}',
  period_start date,
  period_end date,
  notes_mode text,
  updated_at timestamptz not null default now(),
  primary key (office_id, robot_technical_id)
);

create table if not exists public.documents (
  id uuid primary key default gen_random_uuid(),
  office_id uuid not null default public.current_office_id() references public.offices(id) on delete cascade,
  company_id uuid not null references public.companies(id) on delete cascade,
  tipo text not null check (tipo in ('NFS', 'NFE', 'NFC')),
  periodo text not null,
  status public.document_status not null default 'novo',
  origem text not null default 'Automacao',
  document_date date,
  arquivos text[] default '{}',
  created_at timestamptz not null default now()
);

create table if not exists public.fiscal_documents (
  id uuid primary key default gen_random_uuid(),
  office_id uuid not null default public.current_office_id() references public.offices(id) on delete cascade,
  company_id uuid not null references public.companies(id) on delete cascade,
  type text not null check (type in ('NFS', 'NFE', 'NFC')),
  chave text,
  periodo text not null,
  status public.document_status not null default 'novo',
  document_date date,
  file_path text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  last_downloaded_at timestamptz
);

create index if not exists fiscal_documents_office_company_idx on public.fiscal_documents (office_id, company_id, type);
create index if not exists fiscal_documents_office_created_idx on public.fiscal_documents (office_id, created_at desc);
create index if not exists fiscal_documents_office_document_date_idx on public.fiscal_documents (office_id, document_date desc);
create index if not exists fiscal_documents_office_period_type_idx on public.fiscal_documents (office_id, periodo, type);
create index if not exists fiscal_documents_office_status_created_idx on public.fiscal_documents (office_id, status, created_at desc);

create table if not exists public.fiscal_pendencias (
  id uuid primary key default gen_random_uuid(),
  office_id uuid not null default public.current_office_id() references public.offices(id) on delete cascade,
  company_id uuid not null references public.companies(id) on delete cascade,
  tipo text not null check (tipo in ('NFS', 'NFE', 'NFC')),
  periodo text not null,
  status public.document_status not null,
  created_at timestamptz not null default now()
);

create index if not exists fiscal_pendencias_office_status_created_idx on public.fiscal_pendencias (office_id, status, created_at desc);
create index if not exists fiscal_pendencias_office_company_period_idx on public.fiscal_pendencias (office_id, company_id, periodo);

create table if not exists public.dp_checklist (
  id uuid primary key default gen_random_uuid(),
  office_id uuid not null default public.current_office_id() references public.offices(id) on delete cascade,
  company_id uuid not null references public.companies(id) on delete cascade,
  tarefa text not null,
  competencia text not null,
  status public.document_status not null default 'pendente',
  created_at timestamptz not null default now()
);

create index if not exists dp_checklist_office_status_competencia_idx on public.dp_checklist (office_id, status, competencia desc);
create index if not exists dp_checklist_office_company_competencia_idx on public.dp_checklist (office_id, company_id, competencia desc);

create table if not exists public.dp_guias (
  id uuid primary key default gen_random_uuid(),
  office_id uuid not null default public.current_office_id() references public.offices(id) on delete cascade,
  company_id uuid not null references public.companies(id) on delete cascade,
  nome text not null,
  tipo text not null default 'PDF',
  data date not null,
  file_path text,
  created_at timestamptz not null default now()
);

create index if not exists dp_guias_office_data_idx on public.dp_guias (office_id, data desc);
create index if not exists dp_guias_office_company_created_idx on public.dp_guias (office_id, company_id, created_at desc);

create table if not exists public.financial_records (
  id uuid primary key default gen_random_uuid(),
  office_id uuid not null default public.current_office_id() references public.offices(id) on delete cascade,
  company_id uuid not null references public.companies(id) on delete cascade,
  periodo text not null,
  valor_cents bigint not null default 0,
  status public.document_status not null default 'pendente',
  pendencias_count integer not null default 0,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists financial_records_office_period_idx on public.financial_records (office_id, periodo desc);
create index if not exists financial_records_office_status_created_idx on public.financial_records (office_id, status, created_at desc);
create index if not exists financial_records_office_company_period_idx on public.financial_records (office_id, company_id, periodo desc);

create table if not exists public.automation_data (
  id uuid primary key default gen_random_uuid(),
  office_id uuid not null default public.current_office_id() references public.offices(id) on delete cascade,
  company_id uuid not null references public.companies(id) on delete cascade,
  automation_id text not null,
  date date not null,
  count_1 bigint,
  count_2 bigint,
  count_3 bigint,
  amount_1 numeric,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create table if not exists public.ir_settings (
  id uuid primary key default gen_random_uuid(),
  office_id uuid not null unique default public.current_office_id() references public.offices(id) on delete cascade,
  payment_due_date date,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists public.ir_clients (
  id uuid primary key default gen_random_uuid(),
  office_id uuid not null default public.current_office_id() references public.offices(id) on delete cascade,
  nome text not null,
  cpf_cnpj text not null,
  valor_servico numeric not null default 0,
  status_pagamento text not null default 'A PAGAR' check (status_pagamento in ('PIX', 'DINHEIRO', 'TRANSFERENCIA POUPANCA', 'PERMUTA', 'A PAGAR')),
  status_declaracao text not null default 'Pendente' check (status_declaracao in ('Pendente', 'Concluido')),
  observacoes text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  responsavel_ir text,
  vencimento date,
  payment_charge_type text,
  payment_charge_status text not null default 'none' check (payment_charge_status in ('none', 'pending', 'paid', 'failed', 'cancelled')),
  payment_charge_id text,
  payment_charge_correlation_id text,
  payment_provider text,
  payment_link text,
  payment_pix_copy_paste text,
  payment_pix_qr_code text,
  payment_boleto_pdf_base64 text,
  payment_boleto_barcode text,
  payment_boleto_digitable_line text,
  payment_paid_at timestamptz,
  payment_payer_name text,
  payment_payer_tax_id text,
  payment_generated_at timestamptz,
  payment_last_webhook_at timestamptz,
  payment_metadata jsonb not null default '{}'::jsonb
);

update public.accountants
set cpf = public.only_digits(cpf)
where cpf is distinct from public.only_digits(cpf);

update public.companies
set document = nullif(public.only_digits(document), ''),
    contador_cpf = nullif(public.only_digits(contador_cpf), '')
where document is distinct from nullif(public.only_digits(document), '')
   or contador_cpf is distinct from nullif(public.only_digits(contador_cpf), '');

update public.company_robot_config
set selected_login_cpf = nullif(public.only_digits(selected_login_cpf), '')
where selected_login_cpf is distinct from nullif(public.only_digits(selected_login_cpf), '');

update public.ir_clients
set cpf_cnpj = public.only_digits(cpf_cnpj)
where cpf_cnpj is distinct from public.only_digits(cpf_cnpj);

alter table public.accountants
  drop constraint if exists accountants_cpf_valid_check;
alter table public.accountants
  add constraint accountants_cpf_valid_check
  check (public.is_valid_cpf(cpf));

alter table public.companies
  drop constraint if exists companies_document_valid_check;
alter table public.companies
  add constraint companies_document_valid_check
  check (document is null or public.is_valid_cnpj(document));

alter table public.companies
  drop constraint if exists companies_contador_cpf_valid_check;
alter table public.companies
  add constraint companies_contador_cpf_valid_check
  check (contador_cpf is null or public.is_valid_cpf(contador_cpf));

alter table public.companies
  drop constraint if exists companies_sefaz_go_logins_valid_check;
alter table public.companies
  add constraint companies_sefaz_go_logins_valid_check
  check (public.are_valid_portal_logins(sefaz_go_logins));

alter table public.company_robot_config
  drop constraint if exists company_robot_config_selected_login_cpf_valid_check;
alter table public.company_robot_config
  add constraint company_robot_config_selected_login_cpf_valid_check
  check (selected_login_cpf is null or public.is_valid_cpf(selected_login_cpf));

alter table public.robots
  drop constraint if exists robots_global_logins_valid_check;
alter table public.robots
  add constraint robots_global_logins_valid_check
  check (public.are_valid_portal_logins(global_logins));

alter table public.ir_clients
  drop constraint if exists ir_clients_cpf_cnpj_valid_check;
alter table public.ir_clients
  add constraint ir_clients_cpf_cnpj_valid_check
  check (public.is_valid_cpf_or_cnpj(cpf_cnpj));

create index if not exists companies_office_document_idx on public.companies (office_id, document);

drop trigger if exists accountants_normalize_tax_documents on public.accountants;
create trigger accountants_normalize_tax_documents
  before insert or update on public.accountants
  for each row execute procedure public.normalize_tax_document_fields();

drop trigger if exists companies_normalize_tax_documents on public.companies;
create trigger companies_normalize_tax_documents
  before insert or update on public.companies
  for each row execute procedure public.normalize_tax_document_fields();

drop trigger if exists company_robot_config_normalize_tax_documents on public.company_robot_config;
create trigger company_robot_config_normalize_tax_documents
  before insert or update on public.company_robot_config
  for each row execute procedure public.normalize_tax_document_fields();

drop trigger if exists ir_clients_normalize_tax_documents on public.ir_clients;
create trigger ir_clients_normalize_tax_documents
  before insert or update on public.ir_clients
  for each row execute procedure public.normalize_tax_document_fields();

drop trigger if exists companies_prevent_duplicate_document on public.companies;
create trigger companies_prevent_duplicate_document
  before insert or update of office_id, document on public.companies
  for each row execute procedure public.prevent_duplicate_company_document();

create table if not exists public.municipal_tax_collection_runs (
  id uuid primary key default gen_random_uuid(),
  office_id uuid not null default public.current_office_id() references public.offices(id) on delete cascade,
  robot_technical_id text not null,
  company_id uuid references public.companies(id) on delete set null,
  company_name text,
  status text not null check (status in ('pending', 'running', 'completed', 'failed')),
  started_at timestamptz,
  finished_at timestamptz,
  debts_found integer not null default 0,
  error_message text,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists public.municipal_tax_debts (
  id uuid primary key default gen_random_uuid(),
  office_id uuid not null default public.current_office_id() references public.offices(id) on delete cascade,
  company_id uuid not null references public.companies(id) on delete cascade,
  ano integer,
  tributo text not null,
  numero_documento text,
  data_vencimento date,
  valor numeric not null default 0,
  situacao text,
  portal_inscricao text,
  portal_cai text,
  detalhes jsonb not null default '{}'::jsonb,
  fetched_at timestamptz not null default now(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  guia_pdf_path text,
  constraint municipal_tax_debts_dedupe_unique unique (office_id, company_id, tributo, numero_documento, data_vencimento)
);

create index if not exists municipal_tax_debts_company_idx on public.municipal_tax_debts (office_id, company_id);
create index if not exists municipal_tax_debts_due_idx on public.municipal_tax_debts (office_id, data_vencimento);
create index if not exists municipal_tax_debts_status_idx on public.municipal_tax_debts (office_id, situacao);
create index if not exists municipal_tax_collection_runs_robot_idx on public.municipal_tax_collection_runs (office_id, robot_technical_id, created_at desc);
create index if not exists municipal_tax_collection_runs_company_idx on public.municipal_tax_collection_runs (office_id, company_id, created_at desc);

create table if not exists public.nfs_stats (
  id uuid primary key default gen_random_uuid(),
  office_id uuid not null default public.current_office_id() references public.offices(id) on delete cascade,
  company_id uuid not null references public.companies(id) on delete cascade,
  period text not null check (period ~ '^\d{4}-\d{2}$'),
  qty_emitidas integer not null default 0,
  qty_recebidas integer not null default 0,
  valor_emitidas numeric not null default 0,
  valor_recebidas numeric not null default 0,
  service_codes jsonb not null default '[]'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  service_codes_emitidas jsonb not null default '[]'::jsonb,
  service_codes_recebidas jsonb not null default '[]'::jsonb,
  constraint nfs_stats_office_company_period_key unique (office_id, company_id, period)
);

create index if not exists nfs_stats_office_period_idx on public.nfs_stats (office_id, period desc);

create table if not exists public.sync_events (
  id uuid primary key default gen_random_uuid(),
  office_id uuid not null default public.current_office_id() references public.offices(id) on delete cascade,
  company_id uuid references public.companies(id) on delete set null,
  tipo text not null,
  payload text,
  status public.document_status not null,
  idempotency_key text,
  retries integer not null default 0,
  created_at timestamptz not null default now()
);

create index if not exists sync_events_office_created_idx on public.sync_events (office_id, created_at desc);
create index if not exists sync_events_office_tipo_created_idx on public.sync_events (office_id, tipo, created_at desc);

create table if not exists public.tax_rule_versions (
  id uuid primary key default gen_random_uuid(),
  regime text not null,
  scope text not null,
  version_code text not null,
  effective_from date not null,
  effective_to date,
  title text not null,
  source_reference text not null,
  source_url text,
  payload jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint tax_rule_versions_regime_scope_version_code_key unique (regime, scope, version_code)
);

create index if not exists idx_tax_rule_versions_regime_scope on public.tax_rule_versions(regime, scope);

create table if not exists public.simple_national_periods (
  id uuid primary key default gen_random_uuid(),
  office_id uuid not null default public.current_office_id() references public.offices(id) on delete cascade,
  company_id uuid not null references public.companies(id) on delete cascade,
  apuration_period text not null check (apuration_period ~ '^[0-9]{4}-[0-9]{2}$'),
  company_start_date date,
  current_period_revenue numeric not null default 0,
  subject_to_factor_r boolean not null default false,
  base_annex text not null default 'I' check (base_annex in ('I', 'II', 'III', 'IV', 'V')),
  activity_label text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  municipal_iss_rate numeric,
  constraint simple_national_periods_company_id_apuration_period_key unique (company_id, apuration_period)
);

create index if not exists idx_simple_national_periods_company_period on public.simple_national_periods(company_id, apuration_period desc);

create table if not exists public.simple_national_entries (
  id uuid primary key default gen_random_uuid(),
  office_id uuid not null default public.current_office_id() references public.offices(id) on delete cascade,
  period_id uuid not null references public.simple_national_periods(id) on delete cascade,
  company_id uuid not null references public.companies(id) on delete cascade,
  reference_month text not null check (reference_month ~ '^[0-9]{4}-[0-9]{2}$'),
  entry_type text not null check (entry_type in ('revenue', 'payroll')),
  amount numeric not null default 0,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint simple_national_entries_period_ref_type_key unique (period_id, reference_month, entry_type)
);

create index if not exists idx_simple_national_entries_period_type on public.simple_national_entries(period_id, entry_type, reference_month);

create table if not exists public.simple_national_calculations (
  id uuid primary key default gen_random_uuid(),
  office_id uuid not null default public.current_office_id() references public.offices(id) on delete cascade,
  period_id uuid not null unique references public.simple_national_periods(id) on delete cascade,
  company_id uuid not null references public.companies(id) on delete cascade,
  rule_version_code text not null,
  result_payload jsonb not null default '{}'::jsonb,
  memory_payload jsonb not null default '[]'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists idx_simple_national_calculations_company on public.simple_national_calculations(company_id, updated_at desc);

create table if not exists public.simple_national_historical_revenue_allocations (
  id uuid primary key default gen_random_uuid(),
  office_id uuid not null default public.current_office_id() references public.offices(id) on delete cascade,
  period_id uuid not null references public.simple_national_periods(id) on delete cascade,
  company_id uuid not null references public.companies(id) on delete cascade,
  reference_month text not null check (reference_month ~ '^[0-9]{4}-[0-9]{2}$'),
  annex_code text not null check (annex_code in ('I', 'II', 'III', 'IV', 'V')),
  amount numeric not null default 0,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists public.simple_national_payroll_compositions (
  id uuid primary key default gen_random_uuid(),
  office_id uuid not null default public.current_office_id() references public.offices(id) on delete cascade,
  period_id uuid not null unique references public.simple_national_periods(id) on delete cascade,
  company_id uuid not null references public.companies(id) on delete cascade,
  employees_amount numeric not null default 0,
  pro_labore_amount numeric not null default 0,
  individual_contractors_amount numeric not null default 0,
  thirteenth_salary_amount numeric not null default 0,
  employer_cpp_amount numeric not null default 0,
  fgts_amount numeric not null default 0,
  excluded_profit_distribution_amount numeric not null default 0,
  excluded_rent_amount numeric not null default 0,
  excluded_interns_amount numeric not null default 0,
  excluded_mei_amount numeric not null default 0,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists public.simple_national_revenue_segments (
  id uuid primary key default gen_random_uuid(),
  office_id uuid not null default public.current_office_id() references public.offices(id) on delete cascade,
  period_id uuid not null references public.simple_national_periods(id) on delete cascade,
  company_id uuid not null references public.companies(id) on delete cascade,
  segment_code text not null check (segment_code in ('standard', 'annex_ii_ipi_iss', 'I', 'II', 'III', 'IV', 'V')),
  market_type text not null default 'internal' check (market_type in ('internal', 'external')),
  description text,
  amount numeric not null default 0,
  display_order integer not null default 0,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists public.robot_schedules (
  id uuid primary key default gen_random_uuid(),
  office_id uuid not null default public.current_office_id() references public.offices(id) on delete cascade,
  robot_technical_id text not null references public.robots(technical_id) on delete cascade,
  company_ids uuid[] not null default '{}',
  run_at_time time not null,
  run_daily boolean not null default true,
  run_at_date date,
  execution_mode text not null default 'sequential',
  notes_mode text,
  status text not null default 'active' check (status in ('active', 'paused', 'completed')),
  created_by uuid references public.profiles(id),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists public.robot_jobs (
  id uuid primary key default gen_random_uuid(),
  office_id uuid not null default public.current_office_id() references public.offices(id) on delete cascade,
  robot_schedule_id uuid references public.robot_schedules(id) on delete set null,
  robot_technical_id text not null references public.robots(technical_id) on delete cascade,
  company_ids uuid[] not null default '{}',
  status public.robot_job_status not null default 'pending',
  attempt_count integer not null default 0,
  claimed_at timestamptz,
  claimed_by_server_id uuid references public.office_servers(id) on delete set null,
  timeout_at timestamptz,
  last_error text,
  result_payload jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  completed_at timestamptz
);

create index if not exists robot_jobs_office_status_idx on public.robot_jobs (office_id, status, created_at desc);

create table if not exists public.robot_job_logs (
  id uuid primary key default gen_random_uuid(),
  office_id uuid not null default public.current_office_id() references public.offices(id) on delete cascade,
  robot_job_id uuid not null references public.robot_jobs(id) on delete cascade,
  level text not null default 'info',
  message text not null,
  payload jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create or replace function public.copy_default_folder_structure(target_office_id uuid)
returns void
language plpgsql
security definer
set search_path = public
as $$
declare
  row record;
  inserted_id uuid;
  id_map jsonb := '{}'::jsonb;
  mapped_parent uuid;
begin
  for row in
    select * from public.folder_structure_templates
    order by coalesce(parent_id::text, ''), position, created_at
  loop
    mapped_parent := case when row.parent_id is null then null else (id_map ->> row.parent_id::text)::uuid end;
    insert into public.folder_structure_nodes (office_id, parent_id, name, slug, date_rule, position)
    values (target_office_id, mapped_parent, row.name, row.slug, row.date_rule, row.position)
    returning id into inserted_id;
    id_map := id_map || jsonb_build_object(row.id::text, inserted_id::text);
  end loop;
end;
$$;

create or replace function public.ensure_office_branding(target_office_id uuid)
returns void
language plpgsql
security definer
set search_path = public
as $$
begin
  insert into public.office_branding (office_id)
  values (target_office_id)
  on conflict (office_id) do nothing;
end;
$$;

do $$
declare
  tables text[] := array[
    'profiles','offices','office_memberships','office_servers','office_server_credentials','office_branding','robots',
    'folder_structure_templates','folder_structure_nodes','admin_settings','accountants','companies',
    'company_robot_config','schedule_rules','execution_requests','fiscal_documents','financial_records',
    'ir_settings','ir_clients','municipal_tax_collection_runs','municipal_tax_debts','nfs_stats',
    'tax_rule_versions','simple_national_periods','simple_national_entries','simple_national_calculations',
    'simple_national_historical_revenue_allocations','simple_national_payroll_compositions',
    'simple_national_revenue_segments','robot_schedules','robot_jobs'
  ];
  tbl text;
begin
  foreach tbl in array tables loop
    execute format('drop trigger if exists %I_set_updated_at on public.%I', tbl, tbl);
    execute format('create trigger %I_set_updated_at before update on public.%I for each row execute procedure public.set_updated_at()', tbl, tbl);
  end loop;
end $$;

drop trigger if exists profiles_guard_role_update on public.profiles;
create trigger profiles_guard_role_update
  before update on public.profiles
  for each row execute procedure public.guard_profile_role_update();

alter table public.office_audit_logs enable row level security;
alter table public.office_branding enable row level security;
alter table public.office_memberships enable row level security;
alter table public.office_servers enable row level security;
alter table public.office_server_credentials enable row level security;
alter table public.offices enable row level security;
alter table public.profiles enable row level security;
alter table public.robots enable row level security;
alter table public.folder_structure_templates enable row level security;

do $$
declare
  office_tables text[] := array[
    'folder_structure_nodes','admin_settings','accountants','companies','company_robot_config',
    'schedule_rules','execution_requests','robot_display_config','documents','fiscal_documents',
    'fiscal_pendencias','dp_checklist','dp_guias','financial_records','automation_data','ir_settings',
    'ir_clients','municipal_tax_collection_runs','municipal_tax_debts','nfs_stats','sync_events',
    'simple_national_periods','simple_national_entries','simple_national_calculations',
    'simple_national_historical_revenue_allocations','simple_national_payroll_compositions',
    'simple_national_revenue_segments','robot_schedules','robot_jobs','robot_job_logs'
  ];
  tbl text;
begin
  foreach tbl in array office_tables loop
    execute format('alter table public.%I enable row level security', tbl);
    execute format('drop policy if exists %I_select on public.%I', tbl || '_select', tbl);
    execute format('create policy %I_select on public.%I for select to authenticated using (public.can_view_office(office_id))', tbl || '_select', tbl);
    execute format('drop policy if exists %I_write on public.%I', tbl || '_write', tbl);
    execute format('create policy %I_write on public.%I for all to authenticated using (public.can_manage_office(office_id)) with check (public.can_manage_office(office_id))', tbl || '_write', tbl);
  end loop;
end $$;

drop policy if exists profiles_select on public.profiles;
create policy profiles_select on public.profiles
  for select to authenticated
  using (id = auth.uid() or public.is_platform_super_admin());

drop policy if exists profiles_update on public.profiles;
create policy profiles_update on public.profiles
  for update to authenticated
  using (id = auth.uid() or public.is_platform_super_admin())
  with check (id = auth.uid() or public.is_platform_super_admin());

drop policy if exists offices_select on public.offices;
create policy offices_select on public.offices
  for select to authenticated
  using (public.can_view_office(id));

drop policy if exists offices_write on public.offices;
create policy offices_write on public.offices
  for all to authenticated
  using (public.is_platform_super_admin())
  with check (public.is_platform_super_admin());

drop policy if exists office_memberships_select on public.office_memberships;
create policy office_memberships_select on public.office_memberships
  for select to authenticated
  using (user_id = auth.uid() or public.can_manage_office(office_id));

drop policy if exists office_memberships_write on public.office_memberships;
create policy office_memberships_write on public.office_memberships
  for all to authenticated
  using (public.can_manage_office(office_id))
  with check (public.can_manage_office(office_id));

drop policy if exists office_servers_select on public.office_servers;
create policy office_servers_select on public.office_servers
  for select to authenticated
  using (public.can_view_office(office_id));

drop policy if exists office_servers_write on public.office_servers;
create policy office_servers_write on public.office_servers
  for all to authenticated
  using (public.can_manage_office(office_id))
  with check (public.can_manage_office(office_id));

drop policy if exists office_branding_select on public.office_branding;
create policy office_branding_select on public.office_branding
  for select to authenticated
  using (public.can_view_office(office_id));

drop policy if exists office_branding_write on public.office_branding;
create policy office_branding_write on public.office_branding
  for all to authenticated
  using (public.can_manage_office(office_id))
  with check (public.can_manage_office(office_id));

drop policy if exists office_audit_logs_select on public.office_audit_logs;
create policy office_audit_logs_select on public.office_audit_logs
  for select to authenticated
  using (public.can_view_office(office_id));

drop policy if exists office_audit_logs_insert on public.office_audit_logs;
create policy office_audit_logs_insert on public.office_audit_logs
  for insert to authenticated
  with check (public.can_manage_office(office_id) or public.is_platform_super_admin());

drop policy if exists robots_select on public.robots;
create policy robots_select on public.robots
  for select to authenticated
  using (true);

drop policy if exists robots_write on public.robots;
create policy robots_write on public.robots
  for all to authenticated
  using (public.is_platform_super_admin())
  with check (public.is_platform_super_admin());

drop policy if exists folder_structure_templates_select on public.folder_structure_templates;
create policy folder_structure_templates_select on public.folder_structure_templates
  for select to authenticated
  using (true);

drop policy if exists folder_structure_templates_write on public.folder_structure_templates;
create policy folder_structure_templates_write on public.folder_structure_templates
  for all to authenticated
  using (public.is_platform_super_admin())
  with check (public.is_platform_super_admin());

drop policy if exists tax_rule_versions_select on public.tax_rule_versions;
create policy tax_rule_versions_select on public.tax_rule_versions
  for select to authenticated
  using (true);

drop policy if exists tax_rule_versions_write on public.tax_rule_versions;
create policy tax_rule_versions_write on public.tax_rule_versions
  for all to authenticated
  using (public.is_platform_super_admin())
  with check (public.is_platform_super_admin());

insert into storage.buckets (id, name, public, file_size_limit, allowed_mime_types)
values (
  'branding-assets',
  'branding-assets',
  false,
  2097152,
  array['image/png', 'image/svg+xml', 'image/jpeg', 'image/webp', 'image/x-icon']::text[]
)
on conflict (id) do update
  set public = excluded.public,
      file_size_limit = excluded.file_size_limit,
      allowed_mime_types = excluded.allowed_mime_types;

drop policy if exists branding_assets_select on storage.objects;
create policy branding_assets_select on storage.objects
  for select to authenticated
  using (
    bucket_id = 'branding-assets'
    and (
      public.is_platform_super_admin()
      or (storage.foldername(name))[1] = public.current_office_id()::text
    )
  );

drop policy if exists branding_assets_insert on storage.objects;
create policy branding_assets_insert on storage.objects
  for insert to authenticated
  with check (
    bucket_id = 'branding-assets'
    and (
      public.is_platform_super_admin()
      or (
        (storage.foldername(name))[1] = public.current_office_id()::text
        and public.can_manage_office(public.current_office_id())
      )
    )
  );

drop policy if exists branding_assets_update on storage.objects;
create policy branding_assets_update on storage.objects
  for update to authenticated
  using (
    bucket_id = 'branding-assets'
    and (
      public.is_platform_super_admin()
      or (
        (storage.foldername(name))[1] = public.current_office_id()::text
        and public.can_manage_office(public.current_office_id())
      )
    )
  )
  with check (
    bucket_id = 'branding-assets'
    and (
      public.is_platform_super_admin()
      or (
        (storage.foldername(name))[1] = public.current_office_id()::text
        and public.can_manage_office(public.current_office_id())
      )
    )
  );

drop policy if exists branding_assets_delete on storage.objects;
create policy branding_assets_delete on storage.objects
  for delete to authenticated
  using (
    bucket_id = 'branding-assets'
    and (
      public.is_platform_super_admin()
      or (
        (storage.foldername(name))[1] = public.current_office_id()::text
        and public.can_manage_office(public.current_office_id())
      )
    )
  );

insert into public.robots (
  technical_id,
  display_name,
  status,
  segment_path,
  notes_mode,
  date_execution_mode,
  is_fiscal_notes_robot,
  fiscal_notes_kind
)
values
  ('nfs_padrao', 'NFS Padrao', 'active', 'FISCAL/NFS', 'both', 'interval', true, 'nfs'),
  ('sefaz_xml', 'Sefaz XML', 'active', 'FISCAL/NFE-NFC', 'modelos_55_65', 'interval', true, 'nfe_nfc'),
  ('certidoes', 'Certidoes', 'active', 'FISCAL/CERTIDOES', null, 'interval', false, null),
  ('goiania_taxas_impostos', 'Taxas e Impostos Goiania', 'active', 'PARALEGAL/TAXAS-IMPOSTOS', null, 'interval', false, null)
on conflict (technical_id) do update
  set display_name = excluded.display_name,
      status = excluded.status,
      segment_path = excluded.segment_path,
      notes_mode = excluded.notes_mode,
      date_execution_mode = excluded.date_execution_mode,
      is_fiscal_notes_robot = excluded.is_fiscal_notes_robot,
      fiscal_notes_kind = excluded.fiscal_notes_kind,
      updated_at = now();

insert into public.folder_structure_templates (id, parent_id, name, slug, date_rule, position)
values
  ('00000000-0000-0000-0000-000000000101', null, 'Fiscal', 'fiscal', null, 1),
  ('00000000-0000-0000-0000-000000000102', '00000000-0000-0000-0000-000000000101', 'NFS', 'nfs', 'year_month_day', 1),
  ('00000000-0000-0000-0000-000000000103', '00000000-0000-0000-0000-000000000101', 'NFE-NFC', 'nfe-nfc', 'year_month_day', 2),
  ('00000000-0000-0000-0000-000000000104', '00000000-0000-0000-0000-000000000101', 'Certidoes', 'certidoes', 'year_month_day', 3),
  ('00000000-0000-0000-0000-000000000105', null, 'Departamento Pessoal', 'dp', null, 2),
  ('00000000-0000-0000-0000-000000000106', '00000000-0000-0000-0000-000000000105', 'Guias', 'guias', 'year_month_day', 1),
  ('00000000-0000-0000-0000-000000000107', null, 'Paralegal', 'paralegal', null, 3),
  ('00000000-0000-0000-0000-000000000108', '00000000-0000-0000-0000-000000000107', 'Taxas e Impostos', 'taxas-impostos', 'year_month_day', 1)
on conflict (id) do update
  set parent_id = excluded.parent_id,
      name = excluded.name,
      slug = excluded.slug,
      date_rule = excluded.date_rule,
      position = excluded.position,
      updated_at = now();

