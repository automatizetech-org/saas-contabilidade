create extension if not exists pg_trgm with schema extensions;
create extension if not exists unaccent with schema extensions;

create or replace function public.normalize_search_text(value text)
returns text
language sql
stable
as $$
  select lower(
    trim(
      regexp_replace(
        extensions.unaccent(coalesce(value, '')),
        '\s+',
        ' ',
        'g'
      )
    )
  );
$$;

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

create table if not exists public.office_analytics_refresh_queue (
  office_id uuid not null,
  module text not null,
  requested_at timestamptz not null default now(),
  started_at timestamptz,
  last_processed_at timestamptz,
  attempt_count integer not null default 0,
  primary key (office_id, module)
);

create table if not exists public.office_dashboard_daily (
  office_id uuid not null,
  company_id uuid not null,
  day date not null,
  documents_count integer not null default 0,
  imported_documents_count integer not null default 0,
  nfs_count integer not null default 0,
  nfe_count integer not null default 0,
  nfc_count integer not null default 0,
  pending_count integer not null default 0,
  rejected_count integer not null default 0,
  updated_at timestamptz not null default now(),
  primary key (office_id, company_id, day)
);

create table if not exists public.office_fiscal_daily (
  office_id uuid not null,
  company_id uuid not null,
  day date not null,
  type text not null,
  documents_count integer not null default 0,
  available_documents_count integer not null default 0,
  pending_count integer not null default 0,
  rejected_count integer not null default 0,
  updated_at timestamptz not null default now(),
  primary key (office_id, company_id, day, type)
);

create table if not exists public.office_ir_summary (
  office_id uuid primary key,
  summary_payload jsonb not null default '{}'::jsonb,
  updated_at timestamptz not null default now()
);

create table if not exists public.office_municipal_tax_summary (
  office_id uuid primary key,
  summary_payload jsonb not null default '{}'::jsonb,
  updated_at timestamptz not null default now()
);

create table if not exists public.office_certificate_summary (
  office_id uuid primary key,
  summary_payload jsonb not null default '{}'::jsonb,
  updated_at timestamptz not null default now()
);

create table if not exists public.office_tax_intelligence_summary (
  office_id uuid primary key,
  summary_payload jsonb not null default '{}'::jsonb,
  updated_at timestamptz not null default now()
);

create table if not exists public.office_operations_summary (
  office_id uuid primary key,
  summary_payload jsonb not null default '{}'::jsonb,
  updated_at timestamptz not null default now()
);

create index if not exists office_document_index_office_date_idx
  on public.office_document_index (office_id, coalesce(document_date, (created_at at time zone 'UTC')::date) desc, created_at desc, source_record_id desc);

create index if not exists office_document_index_office_category_idx
  on public.office_document_index (office_id, category_key, coalesce(document_date, (created_at at time zone 'UTC')::date) desc, source_record_id desc);

create index if not exists office_document_index_office_company_idx
  on public.office_document_index (office_id, company_id, coalesce(document_date, (created_at at time zone 'UTC')::date) desc, source_record_id desc);

create index if not exists office_document_index_search_trgm_idx
  on public.office_document_index
  using gin (search_text_normalized extensions.gin_trgm_ops);

create index if not exists office_document_index_pdf_idx
  on public.office_document_index (office_id, coalesce(document_date, (created_at at time zone 'UTC')::date) desc, source_record_id desc)
  where file_extension = 'pdf';

create index if not exists office_document_index_xml_idx
  on public.office_document_index (office_id, coalesce(document_date, (created_at at time zone 'UTC')::date) desc, source_record_id desc)
  where file_extension = 'xml';

create index if not exists office_analytics_refresh_queue_requested_idx
  on public.office_analytics_refresh_queue (requested_at asc);

alter table public.office_document_index enable row level security;
alter table public.office_dashboard_daily enable row level security;
alter table public.office_fiscal_daily enable row level security;
alter table public.office_ir_summary enable row level security;
alter table public.office_municipal_tax_summary enable row level security;
alter table public.office_certificate_summary enable row level security;
alter table public.office_tax_intelligence_summary enable row level security;
alter table public.office_operations_summary enable row level security;
alter table public.office_analytics_refresh_queue enable row level security;

drop policy if exists office_document_index_select on public.office_document_index;
create policy office_document_index_select
  on public.office_document_index
  for select
  to authenticated
  using (public.can_view_office(office_id));

drop policy if exists office_document_index_write on public.office_document_index;
create policy office_document_index_write
  on public.office_document_index
  for all
  to authenticated
  using (public.can_manage_office(office_id))
  with check (public.can_manage_office(office_id));

drop policy if exists office_dashboard_daily_select on public.office_dashboard_daily;
create policy office_dashboard_daily_select
  on public.office_dashboard_daily
  for select
  to authenticated
  using (public.can_view_office(office_id));

drop policy if exists office_fiscal_daily_select on public.office_fiscal_daily;
create policy office_fiscal_daily_select
  on public.office_fiscal_daily
  for select
  to authenticated
  using (public.can_view_office(office_id));

drop policy if exists office_ir_summary_select on public.office_ir_summary;
create policy office_ir_summary_select
  on public.office_ir_summary
  for select
  to authenticated
  using (public.can_view_office(office_id));

drop policy if exists office_municipal_tax_summary_select on public.office_municipal_tax_summary;
create policy office_municipal_tax_summary_select
  on public.office_municipal_tax_summary
  for select
  to authenticated
  using (public.can_view_office(office_id));

drop policy if exists office_certificate_summary_select on public.office_certificate_summary;
create policy office_certificate_summary_select
  on public.office_certificate_summary
  for select
  to authenticated
  using (public.can_view_office(office_id));

drop policy if exists office_tax_intelligence_summary_select on public.office_tax_intelligence_summary;
create policy office_tax_intelligence_summary_select
  on public.office_tax_intelligence_summary
  for select
  to authenticated
  using (public.can_view_office(office_id));

drop policy if exists office_operations_summary_select on public.office_operations_summary;
create policy office_operations_summary_select
  on public.office_operations_summary
  for select
  to authenticated
  using (public.can_view_office(office_id));

create or replace function public.office_document_search_blob(
  empresa text,
  cnpj text,
  type text,
  status text,
  periodo text,
  file_path text,
  chave text,
  origem text,
  modelo text default null,
  tipo_certidao text default null
)
returns text
language sql
stable
as $$
  select public.normalize_search_text(
    concat_ws(
      ' ',
      empresa,
      cnpj,
      type,
      status,
      periodo,
      file_path,
      chave,
      origem,
      modelo,
      tipo_certidao,
      public.only_digits(cnpj)
    )
  );
$$;

create or replace function public.upsert_office_document_index_fiscal(p_document_id uuid)
returns void
language plpgsql
security definer
set search_path = public
as $$
begin
  delete from public.office_document_index
  where source = 'fiscal'
    and source_record_id = p_document_id;

  insert into public.office_document_index (
    source,
    source_record_id,
    office_id,
    company_id,
    empresa,
    cnpj,
    category_key,
    type,
    origem,
    status,
    periodo,
    document_date,
    created_at,
    file_path,
    chave,
    modelo,
    tipo_certidao,
    file_extension,
    search_text_normalized,
    updated_at
  )
  select
    'fiscal',
    fd.id,
    fd.office_id,
    fd.company_id,
    c.name,
    c.document,
    case
      when fd.type = 'NFS' then 'nfs'
      when fd.type in ('NFE', 'NFC') then 'nfe_nfc'
      else 'fiscal_outros'
    end,
    fd.type,
    case
      when fd.file_path ilike '%/Recebidas/%' then 'recebidas'
      when fd.file_path ilike '%/Emitidas/%' then 'emitidas'
      else null
    end,
    fd.status::text,
    fd.periodo,
    coalesce(fd.document_date, fd.created_at::date),
    fd.created_at,
    fd.file_path,
    fd.chave,
    case
      when fd.type = 'NFE' then '55'
      when fd.type = 'NFC' then '65'
      else null
    end,
    null::text,
    case
      when lower(coalesce(fd.file_path, '')) like '%.pdf' then 'pdf'
      when lower(coalesce(fd.file_path, '')) like '%.xml' then 'xml'
      else null
    end,
    public.office_document_search_blob(
      c.name,
      c.document,
      fd.type,
      fd.status::text,
      fd.periodo,
      fd.file_path,
      fd.chave,
      case
        when fd.file_path ilike '%/Recebidas/%' then 'recebidas'
        when fd.file_path ilike '%/Emitidas/%' then 'emitidas'
        else null
      end,
      case
        when fd.type = 'NFE' then '55'
        when fd.type = 'NFC' then '65'
        else null
      end,
      null
    ),
    now()
  from public.fiscal_documents fd
  join public.companies c
    on c.id = fd.company_id
   and c.office_id = fd.office_id
  where fd.id = p_document_id
    and coalesce(btrim(fd.file_path), '') <> '';
end;
$$;

create or replace function public.upsert_office_document_index_dp_guia(p_guide_id uuid)
returns void
language plpgsql
security definer
set search_path = public
as $$
begin
  delete from public.office_document_index
  where source = 'dp_guias'
    and source_record_id = p_guide_id;

  insert into public.office_document_index (
    source,
    source_record_id,
    office_id,
    company_id,
    empresa,
    cnpj,
    category_key,
    type,
    origem,
    status,
    periodo,
    document_date,
    created_at,
    file_path,
    chave,
    modelo,
    tipo_certidao,
    file_extension,
    search_text_normalized,
    updated_at
  )
  select
    'dp_guias',
    g.id,
    g.office_id,
    g.company_id,
    c.name,
    c.document,
    'taxas_impostos',
    'GUIA - ' || coalesce(g.tipo, 'OUTROS'),
    null::text,
    null::text,
    left(coalesce(g.data::text, ''), 7),
    g.data::date,
    g.created_at,
    g.file_path,
    null::text,
    null::text,
    null::text,
    case
      when lower(coalesce(g.file_path, '')) like '%.pdf' then 'pdf'
      when lower(coalesce(g.file_path, '')) like '%.xml' then 'xml'
      else null
    end,
    public.office_document_search_blob(
      c.name,
      c.document,
      'GUIA - ' || coalesce(g.tipo, 'OUTROS'),
      null,
      left(coalesce(g.data::text, ''), 7),
      g.file_path,
      null,
      null,
      null,
      null
    ),
    now()
  from public.dp_guias g
  join public.companies c
    on c.id = g.company_id
   and c.office_id = g.office_id
  where g.id = p_guide_id
    and coalesce(btrim(g.file_path), '') <> '';
end;
$$;

create or replace function public.upsert_office_document_index_municipal_tax(p_debt_id uuid)
returns void
language plpgsql
security definer
set search_path = public
as $$
begin
  delete from public.office_document_index
  where source = 'municipal_taxes'
    and source_record_id = p_debt_id;

  insert into public.office_document_index (
    source,
    source_record_id,
    office_id,
    company_id,
    empresa,
    cnpj,
    category_key,
    type,
    origem,
    status,
    periodo,
    document_date,
    created_at,
    file_path,
    chave,
    modelo,
    tipo_certidao,
    file_extension,
    search_text_normalized,
    updated_at
  )
  select
    'municipal_taxes',
    d.id,
    d.office_id,
    d.company_id,
    c.name,
    c.document,
    'taxas_impostos',
    'IMPOSTO/TAXA - ' || coalesce(d.tributo, 'OUTROS'),
    null::text,
    case
      when coalesce(d.valor, 0) = 0 then 'regular'
      when d.data_vencimento is null then 'regular'
      when d.data_vencimento < current_date then 'vencido'
      when d.data_vencimento <= current_date + 30 then 'a_vencer'
      else 'regular'
    end,
    left(coalesce(d.data_vencimento::text, ''), 7),
    d.data_vencimento,
    coalesce(d.fetched_at, d.created_at),
    d.guia_pdf_path,
    null::text,
    null::text,
    null::text,
    case
      when lower(coalesce(d.guia_pdf_path, '')) like '%.pdf' then 'pdf'
      when lower(coalesce(d.guia_pdf_path, '')) like '%.xml' then 'xml'
      else null
    end,
    public.office_document_search_blob(
      c.name,
      c.document,
      'IMPOSTO/TAXA - ' || coalesce(d.tributo, 'OUTROS'),
      case
        when coalesce(d.valor, 0) = 0 then 'regular'
        when d.data_vencimento is null then 'regular'
        when d.data_vencimento < current_date then 'vencido'
        when d.data_vencimento <= current_date + 30 then 'a_vencer'
        else 'regular'
      end,
      left(coalesce(d.data_vencimento::text, ''), 7),
      d.guia_pdf_path,
      null,
      null,
      null,
      null
    ),
    now()
  from public.municipal_tax_debts d
  join public.companies c
    on c.id = d.company_id
   and c.office_id = d.office_id
  where d.id = p_debt_id
    and coalesce(btrim(d.guia_pdf_path), '') <> '';
end;
$$;

create or replace function public.refresh_office_document_index_certidao_company(p_office_id uuid, p_company_id uuid)
returns void
language plpgsql
security definer
set search_path = public
as $$
begin
  delete from public.office_document_index
  where source = 'certidoes'
    and office_id = p_office_id
    and company_id = p_company_id;

  insert into public.office_document_index (
    source,
    source_record_id,
    office_id,
    company_id,
    empresa,
    cnpj,
    category_key,
    type,
    origem,
    status,
    periodo,
    document_date,
    created_at,
    file_path,
    chave,
    modelo,
    tipo_certidao,
    file_extension,
    search_text_normalized,
    updated_at
  )
  with certs_raw as (
    select
      se.id,
      se.office_id,
      se.company_id,
      se.created_at,
      public.try_parse_jsonb(se.payload) as payload
    from public.sync_events se
    where se.office_id = p_office_id
      and se.company_id = p_company_id
      and se.tipo = 'certidao_resultado'
  ),
  latest as (
    select distinct on (company_id, coalesce(payload ->> 'tipo_certidao', ''))
      id,
      office_id,
      company_id,
      created_at,
      payload
    from certs_raw
    where coalesce(payload ->> 'tipo_certidao', '') <> ''
    order by company_id, coalesce(payload ->> 'tipo_certidao', ''), created_at desc
  )
  select
    'certidoes',
    l.id,
    l.office_id,
    l.company_id,
    c.name,
    c.document,
    'certidoes',
    'CERTIDÃO - ' ||
      case lower(coalesce(l.payload ->> 'tipo_certidao', ''))
        when 'federal' then 'Federal'
        when 'fgts' then 'FGTS'
        when 'estadual_go' then 'Estadual (GO)'
        else coalesce(l.payload ->> 'tipo_certidao', 'Outra')
      end,
    null::text,
    case
      when lower(coalesce(l.payload ->> 'status', '')) in ('regular', 'negativa') then 'negativa'
      when lower(coalesce(l.payload ->> 'status', '')) = 'positiva' then 'positiva'
      else 'irregular'
    end,
    nullif(btrim(coalesce(l.payload ->> 'periodo', '')), ''),
    coalesce(nullif(l.payload ->> 'document_date', ''), nullif(l.payload ->> 'data_consulta', ''))::date,
    l.created_at,
    nullif(btrim(coalesce(l.payload ->> 'arquivo_pdf', '')), ''),
    null::text,
    null::text,
    lower(coalesce(l.payload ->> 'tipo_certidao', '')),
    case
      when lower(coalesce(l.payload ->> 'arquivo_pdf', '')) like '%.pdf' then 'pdf'
      when lower(coalesce(l.payload ->> 'arquivo_pdf', '')) like '%.xml' then 'xml'
      else null
    end,
    public.office_document_search_blob(
      c.name,
      c.document,
      'CERTIDÃO - ' ||
        case lower(coalesce(l.payload ->> 'tipo_certidao', ''))
          when 'federal' then 'Federal'
          when 'fgts' then 'FGTS'
          when 'estadual_go' then 'Estadual (GO)'
          else coalesce(l.payload ->> 'tipo_certidao', 'Outra')
        end,
      case
        when lower(coalesce(l.payload ->> 'status', '')) in ('regular', 'negativa') then 'negativa'
        when lower(coalesce(l.payload ->> 'status', '')) = 'positiva' then 'positiva'
        else 'irregular'
      end,
      nullif(btrim(coalesce(l.payload ->> 'periodo', '')), ''),
      nullif(btrim(coalesce(l.payload ->> 'arquivo_pdf', '')), ''),
      null,
      null,
      null,
      lower(coalesce(l.payload ->> 'tipo_certidao', ''))
    ),
    now()
  from latest l
  join public.companies c
    on c.id = l.company_id
   and c.office_id = l.office_id
  where coalesce(btrim(coalesce(l.payload ->> 'arquivo_pdf', '')), '') <> '';
end;
$$;

create or replace function public.refresh_office_document_index(p_office_id uuid)
returns void
language plpgsql
security definer
set search_path = public
as $$
declare
  fiscal_row record;
  guia_row record;
  debt_row record;
  company_row record;
begin
  delete from public.office_document_index
  where office_id = p_office_id;

  for fiscal_row in
    select id
    from public.fiscal_documents
    where office_id = p_office_id
  loop
    perform public.upsert_office_document_index_fiscal(fiscal_row.id);
  end loop;

  for guia_row in
    select id
    from public.dp_guias
    where office_id = p_office_id
  loop
    perform public.upsert_office_document_index_dp_guia(guia_row.id);
  end loop;

  for debt_row in
    select id
    from public.municipal_tax_debts
    where office_id = p_office_id
  loop
    perform public.upsert_office_document_index_municipal_tax(debt_row.id);
  end loop;

  for company_row in
    select id
    from public.companies
    where office_id = p_office_id
  loop
    perform public.refresh_office_document_index_certidao_company(p_office_id, company_row.id);
  end loop;
end;
$$;

create or replace function public.refresh_office_dashboard_daily(p_office_id uuid)
returns void
language sql
security definer
set search_path = public
as $$
  delete from public.office_dashboard_daily where office_id = p_office_id;

  insert into public.office_dashboard_daily (
    office_id,
    company_id,
    day,
    documents_count,
    imported_documents_count,
    nfs_count,
    nfe_count,
    nfc_count,
    pending_count,
    rejected_count,
    updated_at
  )
  select
    office_id,
    company_id,
    coalesce(document_date, created_at::date) as day,
    count(*)::int,
    count(*) filter (where coalesce(btrim(file_path), '') <> '')::int,
    count(*) filter (where source = 'fiscal' and type = 'NFS')::int,
    count(*) filter (where source = 'fiscal' and type = 'NFE')::int,
    count(*) filter (where source = 'fiscal' and type = 'NFC')::int,
    count(*) filter (where lower(coalesce(status, '')) in ('pendente', 'processando', 'divergente'))::int,
    count(*) filter (where lower(coalesce(status, '')) in ('rejeitado', 'rejected', 'cancelado', 'cancelada'))::int,
    now()
  from public.office_document_index
  where office_id = p_office_id
  group by office_id, company_id, coalesce(document_date, created_at::date);
$$;

create or replace function public.refresh_office_fiscal_daily(p_office_id uuid)
returns void
language sql
security definer
set search_path = public
as $$
  delete from public.office_fiscal_daily where office_id = p_office_id;

  insert into public.office_fiscal_daily (
    office_id,
    company_id,
    day,
    type,
    documents_count,
    available_documents_count,
    pending_count,
    rejected_count,
    updated_at
  )
  select
    office_id,
    company_id,
    coalesce(document_date, created_at::date) as day,
    type,
    count(*)::int,
    count(*) filter (where coalesce(btrim(file_path), '') <> '')::int,
    count(*) filter (where lower(coalesce(status, '')) in ('pendente', 'processando', 'divergente'))::int,
    count(*) filter (where lower(coalesce(status, '')) in ('rejeitado', 'rejected', 'cancelado', 'cancelada'))::int,
    now()
  from public.office_document_index
  where office_id = p_office_id
    and source = 'fiscal'
  group by office_id, company_id, coalesce(document_date, created_at::date), type;
$$;

create or replace function public.refresh_office_certificate_summary(p_office_id uuid)
returns void
language sql
security definer
set search_path = public
as $$
  insert into public.office_certificate_summary (office_id, summary_payload, updated_at)
  select
    p_office_id,
    jsonb_build_object(
      'cards',
      jsonb_build_object(
        'total', count(*)::int,
        'negativas', count(*) filter (where status = 'negativa')::int,
        'irregulares', count(*) filter (where status <> 'negativa')::int
      ),
      'chartData',
      jsonb_build_array(
        jsonb_build_object('name', 'Negativas', 'value', count(*) filter (where status = 'negativa')::int),
        jsonb_build_object('name', 'Irregulares', 'value', count(*) filter (where status <> 'negativa')::int)
      )
    ),
    now()
  from public.office_document_index
  where office_id = p_office_id
    and source = 'certidoes'
  on conflict (office_id)
  do update set
    summary_payload = excluded.summary_payload,
    updated_at = excluded.updated_at;
$$;

create or replace function public.refresh_office_ir_summary(p_office_id uuid)
returns void
language sql
security definer
set search_path = public
as $$
  with summary as (
    select
      count(*)::int as total,
      count(*) filter (where status_pagamento <> 'A PAGAR')::int as paid_count,
      count(*) filter (where status_pagamento = 'A PAGAR')::int as pending_count,
      count(*) filter (where status_declaracao = 'Concluido')::int as concluded_count,
      count(*) filter (where status_declaracao <> 'Concluido')::int as pending_execution_count,
      coalesce(sum(valor_servico), 0)::numeric as total_value,
      coalesce(sum(valor_servico) filter (where status_pagamento <> 'A PAGAR'), 0)::numeric as paid_value,
      coalesce(sum(valor_servico) filter (where status_pagamento = 'A PAGAR'), 0)::numeric as pending_value
    from public.ir_clients
    where office_id = p_office_id
  )
  insert into public.office_ir_summary (office_id, summary_payload, updated_at)
  select
    p_office_id,
    jsonb_build_object(
      'cards',
      jsonb_build_object(
        'clientesIr', coalesce((select total from summary), 0),
        'recebidos', coalesce((select paid_count from summary), 0),
        'aPagar', coalesce((select pending_count from summary), 0),
        'concluidoPercent', case when coalesce((select total from summary), 0) = 0 then 0 else round((coalesce((select concluded_count from summary), 0)::numeric / (select total from summary)::numeric) * 100) end,
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
      'paidValuePercent', case when coalesce((select total_value from summary), 0) = 0 then 0 else round((coalesce((select paid_value from summary), 0) / (select total_value from summary)) * 100) end
    ),
    now()
  on conflict (office_id)
  do update set
    summary_payload = excluded.summary_payload,
    updated_at = excluded.updated_at;
$$;

create or replace function public.refresh_office_municipal_tax_summary(p_office_id uuid)
returns void
language sql
security definer
set search_path = public
as $$
  with base as (
    select
      d.company_id,
      case
        when coalesce(d.valor, 0) = 0 then 'regular'
        when d.data_vencimento is null then 'regular'
        when d.data_vencimento < current_date then 'vencido'
        when d.data_vencimento <= current_date + 30 then 'a_vencer'
        else 'regular'
      end as status_class,
      d.valor
    from public.municipal_tax_debts d
    where d.office_id = p_office_id
  ),
  summary as (
    select
      count(*)::int as quantidade_debitos,
      count(distinct company_id) filter (where status_class = 'vencido')::int as empresas_com_vencidos,
      count(distinct company_id) filter (where status_class = 'a_vencer')::int as empresas_proximas_vencimento,
      coalesce(sum(valor), 0)::numeric as total_valor,
      coalesce(sum(valor) filter (where status_class = 'vencido'), 0)::numeric as total_vencido,
      coalesce(sum(valor) filter (where status_class = 'a_vencer'), 0)::numeric as total_a_vencer
    from base
  )
  insert into public.office_municipal_tax_summary (office_id, summary_payload, updated_at)
  select
    p_office_id,
    jsonb_build_object(
      'cards',
      jsonb_build_object(
        'totalValor', coalesce((select total_valor from summary), 0),
        'totalVencido', coalesce((select total_vencido from summary), 0),
        'totalAVencer', coalesce((select total_a_vencer from summary), 0),
        'quantidadeDebitos', coalesce((select quantidade_debitos from summary), 0),
        'empresasComVencidos', coalesce((select empresas_com_vencidos from summary), 0),
        'empresasProximasVencimento', coalesce((select empresas_proximas_vencimento from summary), 0)
      )
    ),
    now()
  on conflict (office_id)
  do update set
    summary_payload = excluded.summary_payload,
    updated_at = excluded.updated_at;
$$;

create or replace function public.refresh_office_tax_intelligence_summary(p_office_id uuid)
returns void
language sql
security definer
set search_path = public
as $$
  with rows as (
    select
      c.company_id,
      coalesce(nullif(c.result_payload ->> 'estimatedDas', ''), '0')::numeric as estimated_das,
      coalesce(nullif(c.result_payload ->> 'effectiveRate', ''), '0')::numeric as effective_rate
    from public.simple_national_calculations c
    where c.office_id = p_office_id
  ),
  summary as (
    select
      count(*)::int as total_rows,
      count(distinct company_id)::int as empresas_ativas,
      coalesce(avg(estimated_das), 0)::numeric as media_das,
      coalesce(avg(effective_rate), 0)::numeric as media_aliquota
    from rows
  )
  insert into public.office_tax_intelligence_summary (office_id, summary_payload, updated_at)
  select
    p_office_id,
    jsonb_build_object(
      'cards',
      jsonb_build_object(
        'calculosSalvos', coalesce((select total_rows from summary), 0),
        'mediaDas', coalesce((select media_das from summary), 0),
        'mediaAliquotaEfetiva', coalesce((select media_aliquota from summary), 0),
        'empresasAtivas', coalesce((select empresas_ativas from summary), 0)
      )
    ),
    now()
  on conflict (office_id)
  do update set
    summary_payload = excluded.summary_payload,
    updated_at = excluded.updated_at;
$$;

create or replace function public.refresh_office_operations_summary(p_office_id uuid)
returns void
language sql
security definer
set search_path = public
as $$
  with completed as (
    select *
    from public.execution_requests
    where office_id = p_office_id
      and status in ('completed', 'failed')
  ),
  summary as (
    select
      count(*) filter (where coalesce(completed_at, created_at) >= date_trunc('day', now()))::int as eventos_hoje,
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
  insert into public.office_operations_summary (office_id, summary_payload, updated_at)
  select
    p_office_id,
    jsonb_build_object(
      'eventosHoje', coalesce((select eventos_hoje from summary), 0),
      'eventosOntem', coalesce((select eventos_ontem from summary), 0),
      'falhas', coalesce((select fail_count from summary), 0),
      'robots', coalesce((select robots_count from robot_summary), 0),
      'taxaSucesso', case when coalesce((select total_count from summary), 0) = 0 then 0 else round((coalesce((select success_count from summary), 0)::numeric / (select total_count from summary)::numeric) * 1000) / 10 end
    ),
    now()
  on conflict (office_id)
  do update set
    summary_payload = excluded.summary_payload,
    updated_at = excluded.updated_at;
$$;

create or replace function public.enqueue_office_refresh(p_office_id uuid, p_module text default 'all')
returns void
language sql
security definer
set search_path = public
as $$
  insert into public.office_analytics_refresh_queue (office_id, module, requested_at, started_at, last_processed_at, attempt_count)
  values (p_office_id, lower(coalesce(p_module, 'all')), now(), null, null, 0)
  on conflict (office_id, module)
  do update set
    requested_at = excluded.requested_at,
    started_at = null;
$$;

create or replace function public.process_office_refresh_queue(p_limit integer default 25)
returns integer
language plpgsql
security definer
set search_path = public
as $$
declare
  job record;
  processed_count integer := 0;
begin
  for job in
    select office_id, module
    from public.office_analytics_refresh_queue
    order by requested_at asc
    limit greatest(coalesce(p_limit, 25), 1)
    for update skip locked
  loop
    update public.office_analytics_refresh_queue
       set started_at = now(),
           attempt_count = attempt_count + 1
     where office_id = job.office_id
       and module = job.module;

    if job.module in ('all', 'documents', 'dashboard', 'fiscal', 'certidoes') then
      perform public.refresh_office_document_index(job.office_id);
      perform public.refresh_office_dashboard_daily(job.office_id);
      perform public.refresh_office_fiscal_daily(job.office_id);
      perform public.refresh_office_certificate_summary(job.office_id);
    end if;

    if job.module in ('all', 'ir') then
      perform public.refresh_office_ir_summary(job.office_id);
    end if;

    if job.module in ('all', 'municipal_taxes') then
      perform public.refresh_office_municipal_tax_summary(job.office_id);
    end if;

    if job.module in ('all', 'tax_intelligence') then
      perform public.refresh_office_tax_intelligence_summary(job.office_id);
    end if;

    if job.module in ('all', 'operations') then
      perform public.refresh_office_operations_summary(job.office_id);
    end if;

    update public.office_analytics_refresh_queue
       set last_processed_at = now(),
           started_at = null
     where office_id = job.office_id
       and module = job.module;

    processed_count := processed_count + 1;
  end loop;

  return processed_count;
end;
$$;

create or replace function public.handle_index_projection_change()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
begin
  if tg_table_name = 'fiscal_documents' then
    if tg_op = 'DELETE' then
      delete from public.office_document_index where source = 'fiscal' and source_record_id = old.id;
      perform public.enqueue_office_refresh(old.office_id, 'dashboard');
      return old;
    end if;
    perform public.upsert_office_document_index_fiscal(new.id);
    perform public.enqueue_office_refresh(new.office_id, 'dashboard');
    return new;
  end if;

  if tg_table_name = 'dp_guias' then
    if tg_op = 'DELETE' then
      delete from public.office_document_index where source = 'dp_guias' and source_record_id = old.id;
      perform public.enqueue_office_refresh(old.office_id, 'documents');
      return old;
    end if;
    perform public.upsert_office_document_index_dp_guia(new.id);
    perform public.enqueue_office_refresh(new.office_id, 'documents');
    return new;
  end if;

  if tg_table_name = 'municipal_tax_debts' then
    if tg_op = 'DELETE' then
      delete from public.office_document_index where source = 'municipal_taxes' and source_record_id = old.id;
      perform public.enqueue_office_refresh(old.office_id, 'municipal_taxes');
      return old;
    end if;
    perform public.upsert_office_document_index_municipal_tax(new.id);
    perform public.enqueue_office_refresh(new.office_id, 'municipal_taxes');
    return new;
  end if;

  return coalesce(new, old);
end;
$$;

create or replace function public.handle_sync_event_projection_change()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
declare
  target_office uuid;
  target_company uuid;
begin
  target_office := coalesce(new.office_id, old.office_id);
  target_company := coalesce(new.company_id, old.company_id);

  if coalesce(new.tipo, old.tipo) <> 'certidao_resultado' then
    return coalesce(new, old);
  end if;

  if target_company is not null then
    perform public.refresh_office_document_index_certidao_company(target_office, target_company);
  else
    perform public.refresh_office_document_index(target_office);
  end if;

  perform public.enqueue_office_refresh(target_office, 'certidoes');
  return coalesce(new, old);
end;
$$;

create or replace function public.handle_summary_refresh_enqueue()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
begin
  if tg_table_name = 'ir_clients' then
    perform public.enqueue_office_refresh(coalesce(new.office_id, old.office_id), 'ir');
  elsif tg_table_name = 'simple_national_calculations' then
    perform public.enqueue_office_refresh(coalesce(new.office_id, old.office_id), 'tax_intelligence');
  elsif tg_table_name = 'execution_requests' then
    perform public.enqueue_office_refresh(coalesce(new.office_id, old.office_id), 'operations');
  elsif tg_table_name = 'companies' then
    perform public.enqueue_office_refresh(coalesce(new.office_id, old.office_id), 'certidoes');
    perform public.enqueue_office_refresh(coalesce(new.office_id, old.office_id), 'municipal_taxes');
  end if;
  return coalesce(new, old);
end;
$$;

drop trigger if exists fiscal_documents_projection_refresh on public.fiscal_documents;
create trigger fiscal_documents_projection_refresh
after insert or update or delete on public.fiscal_documents
for each row execute procedure public.handle_index_projection_change();

drop trigger if exists dp_guias_projection_refresh on public.dp_guias;
create trigger dp_guias_projection_refresh
after insert or update or delete on public.dp_guias
for each row execute procedure public.handle_index_projection_change();

drop trigger if exists municipal_tax_debts_projection_refresh on public.municipal_tax_debts;
create trigger municipal_tax_debts_projection_refresh
after insert or update or delete on public.municipal_tax_debts
for each row execute procedure public.handle_index_projection_change();

drop trigger if exists sync_events_projection_refresh on public.sync_events;
create trigger sync_events_projection_refresh
after insert or update or delete on public.sync_events
for each row execute procedure public.handle_sync_event_projection_change();

drop trigger if exists ir_clients_summary_refresh on public.ir_clients;
create trigger ir_clients_summary_refresh
after insert or update or delete on public.ir_clients
for each row execute procedure public.handle_summary_refresh_enqueue();

drop trigger if exists simple_national_calculations_summary_refresh on public.simple_national_calculations;
create trigger simple_national_calculations_summary_refresh
after insert or update or delete on public.simple_national_calculations
for each row execute procedure public.handle_summary_refresh_enqueue();

drop trigger if exists execution_requests_summary_refresh on public.execution_requests;
create trigger execution_requests_summary_refresh
after insert or update or delete on public.execution_requests
for each row execute procedure public.handle_summary_refresh_enqueue();

drop trigger if exists companies_summary_refresh on public.companies;
create trigger companies_summary_refresh
after insert or update on public.companies
for each row execute procedure public.handle_summary_refresh_enqueue();

create or replace function public.get_document_rows_cursor(
  company_ids uuid[] default null,
  category_filter text default null,
  file_kind text default null,
  search_text text default null,
  date_from date default null,
  date_to date default null,
  cursor_sort_date date default null,
  cursor_created_at timestamptz default null,
  cursor_id uuid default null,
  limit_count integer default 25
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_office_id uuid := public.current_office_id();
  v_has_index boolean;
  v_payload jsonb;
begin
  select exists(
    select 1
    from public.office_document_index
    where office_id = v_office_id
    limit 1
  ) into v_has_index;

  if not v_has_index then
    perform public.refresh_office_document_index(v_office_id);
  end if;

  with params as (
    select
      coalesce(company_ids, '{}'::uuid[]) as company_ids,
      coalesce(array_length(company_ids, 1), 0) as company_count,
      lower(coalesce(nullif(btrim(category_filter), ''), 'todos')) as category_filter,
      lower(coalesce(nullif(btrim(file_kind), ''), 'todos')) as file_kind,
      public.normalize_search_text(search_text) as search_text,
      public.only_digits(search_text) as digits_search,
      least(date_from, date_to) as date_from,
      greatest(date_from, date_to) as date_to,
      greatest(least(coalesce(limit_count, 25), 200), 1) as limit_count
  ),
  base as (
    select
      i.*
    from public.office_document_index i
    join params p on true
    where i.office_id = v_office_id
      and (p.company_count = 0 or i.company_id = any (p.company_ids))
      and (p.category_filter = 'todos' or i.category_key = p.category_filter)
      and (
        p.file_kind = 'todos'
        or (p.file_kind = 'xml' and i.file_extension = 'xml')
        or (p.file_kind = 'pdf' and i.file_extension = 'pdf')
      )
      and (p.date_from is null or coalesce(i.document_date, i.created_at::date) >= p.date_from)
      and (p.date_to is null or coalesce(i.document_date, i.created_at::date) <= p.date_to)
      and (
        p.search_text = ''
        or i.search_text_normalized like '%' || p.search_text || '%'
        or (p.digits_search <> '' and public.only_digits(coalesce(i.cnpj, '')) like '%' || p.digits_search || '%')
        or coalesce(i.chave, '') like '%' || btrim(coalesce(search_text, '')) || '%'
      )
      and (
        cursor_sort_date is null
        or (
          (coalesce(i.document_date, i.created_at::date), i.created_at, i.source_record_id)
          < (cursor_sort_date, coalesce(cursor_created_at, 'infinity'::timestamptz), coalesce(cursor_id, 'ffffffff-ffff-ffff-ffff-ffffffffffff'::uuid))
        )
      )
    order by coalesce(i.document_date, i.created_at::date) desc, i.created_at desc, i.source_record_id desc
    limit (select limit_count + 1 from params)
  ),
  visible as (
    select * from base
    limit (select limit_count from params)
  ),
  cursor_row as (
    select
      coalesce(document_date, created_at::date) as sort_date,
      created_at,
      source_record_id
    from visible
    order by coalesce(document_date, created_at::date) asc, created_at asc, source_record_id asc
    limit 1
  )
  select jsonb_build_object(
    'items',
    coalesce((
      select jsonb_agg(
        jsonb_build_object(
          'id', source_record_id,
          'company_id', company_id,
          'empresa', empresa,
          'cnpj', cnpj,
          'source', source,
          'category_key', category_key,
          'type', type,
          'origem', origem,
          'status', status,
          'periodo', periodo,
          'document_date', document_date,
          'created_at', created_at,
          'file_path', file_path,
          'chave', chave
        )
        order by coalesce(document_date, created_at::date) desc, created_at desc, source_record_id desc
      )
      from visible
    ), '[]'::jsonb),
    'nextCursor',
    case
      when (select count(*) from base) > (select limit_count from params) then (
        select jsonb_build_object(
          'sortDate', sort_date,
          'createdAt', created_at,
          'id', source_record_id
        )
        from cursor_row
      )
      else null
    end,
    'hasMore', ((select count(*) from base) > (select limit_count from params)),
    'refreshAt', now()
  )
  into v_payload;

  return coalesce(v_payload, jsonb_build_object('items', '[]'::jsonb, 'nextCursor', null, 'hasMore', false, 'refreshAt', now()));
end;
$$;

create or replace function public.get_fiscal_detail_documents_cursor(
  detail_kind text,
  company_ids uuid[] default null,
  search_text text default null,
  date_from date default null,
  date_to date default null,
  file_kind text default null,
  origem_filter text default null,
  modelo_filter text default null,
  certidao_tipo_filter text default null,
  cursor_sort_date date default null,
  cursor_created_at timestamptz default null,
  cursor_id uuid default null,
  limit_count integer default 25
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_office_id uuid := public.current_office_id();
  v_kind text := upper(coalesce(detail_kind, 'NFS'));
  v_has_index boolean;
  v_payload jsonb;
begin
  select exists(select 1 from public.office_document_index where office_id = v_office_id limit 1)
  into v_has_index;

  if not v_has_index then
    perform public.refresh_office_document_index(v_office_id);
  end if;

  with params as (
    select
      coalesce(company_ids, '{}'::uuid[]) as company_ids,
      coalesce(array_length(company_ids, 1), 0) as company_count,
      public.normalize_search_text(search_text) as search_text,
      public.only_digits(search_text) as digits_search,
      least(date_from, date_to) as date_from,
      greatest(date_from, date_to) as date_to,
      lower(coalesce(nullif(btrim(file_kind), ''), 'all')) as file_kind,
      lower(coalesce(nullif(btrim(origem_filter), ''), 'all')) as origem_filter,
      lower(coalesce(nullif(btrim(modelo_filter), ''), 'all')) as modelo_filter,
      lower(coalesce(nullif(btrim(certidao_tipo_filter), ''), 'all')) as certidao_tipo_filter,
      greatest(least(coalesce(limit_count, 25), 200), 1) as limit_count
  ),
  base as (
    select
      i.*
    from public.office_document_index i
    join params p on true
    where i.office_id = v_office_id
      and (
        (v_kind = 'CERTIDOES' and i.source = 'certidoes')
        or (v_kind = 'NFS' and i.source = 'fiscal' and i.type = 'NFS')
        or (v_kind = 'NFE' and i.source = 'fiscal' and i.type = 'NFE')
        or (v_kind = 'NFC' and i.source = 'fiscal' and i.type = 'NFC')
        or (v_kind = 'NFE_NFC' and i.source = 'fiscal' and i.type in ('NFE', 'NFC'))
      )
      and (p.company_count = 0 or i.company_id = any (p.company_ids))
      and (p.date_from is null or coalesce(i.document_date, i.created_at::date) >= p.date_from)
      and (p.date_to is null or coalesce(i.document_date, i.created_at::date) <= p.date_to)
      and (
        p.search_text = ''
        or i.search_text_normalized like '%' || p.search_text || '%'
        or (p.digits_search <> '' and public.only_digits(coalesce(i.cnpj, '')) like '%' || p.digits_search || '%')
        or coalesce(i.chave, '') like '%' || btrim(coalesce(search_text, '')) || '%'
      )
      and (
        p.file_kind = 'all'
        or (p.file_kind = 'xml' and i.file_extension = 'xml')
        or (p.file_kind = 'pdf' and i.file_extension = 'pdf')
      )
      and (
        p.origem_filter = 'all'
        or v_kind <> 'NFS'
        or coalesce(i.origem, '') = p.origem_filter
      )
      and (
        p.modelo_filter = 'all'
        or v_kind <> 'NFE_NFC'
        or coalesce(i.modelo, '') = p.modelo_filter
      )
      and (
        p.certidao_tipo_filter = 'all'
        or v_kind <> 'CERTIDOES'
        or coalesce(i.tipo_certidao, '') = p.certidao_tipo_filter
      )
      and (
        cursor_sort_date is null
        or (
          (coalesce(i.document_date, i.created_at::date), i.created_at, i.source_record_id)
          < (cursor_sort_date, coalesce(cursor_created_at, 'infinity'::timestamptz), coalesce(cursor_id, 'ffffffff-ffff-ffff-ffff-ffffffffffff'::uuid))
        )
      )
    order by coalesce(i.document_date, i.created_at::date) desc, i.created_at desc, i.source_record_id desc
    limit (select limit_count + 1 from params)
  ),
  visible as (
    select * from base
    limit (select limit_count from params)
  ),
  cursor_row as (
    select
      coalesce(document_date, created_at::date) as sort_date,
      created_at,
      source_record_id
    from visible
    order by coalesce(document_date, created_at::date) asc, created_at asc, source_record_id asc
    limit 1
  )
  select jsonb_build_object(
    'items',
    coalesce((
      select jsonb_agg(
        jsonb_build_object(
          'id', source_record_id,
          'company_id', company_id,
          'empresa', empresa,
          'cnpj', cnpj,
          'type', type,
          'chave', chave,
          'periodo', periodo,
          'status', status,
          'document_date', document_date,
          'created_at', created_at,
          'file_path', file_path,
          'origem', origem,
          'modelo', modelo,
          'tipo_certidao', tipo_certidao
        )
        order by coalesce(document_date, created_at::date) desc, created_at desc, source_record_id desc
      )
      from visible
    ), '[]'::jsonb),
    'nextCursor',
    case
      when (select count(*) from base) > (select limit_count from params) then (
        select jsonb_build_object(
          'sortDate', sort_date,
          'createdAt', created_at,
          'id', source_record_id
        )
        from cursor_row
      )
      else null
    end,
    'hasMore', ((select count(*) from base) > (select limit_count from params)),
    'refreshAt', now()
  )
  into v_payload;

  return coalesce(v_payload, jsonb_build_object('items', '[]'::jsonb, 'nextCursor', null, 'hasMore', false, 'refreshAt', now()));
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
    public.normalize_search_text(search_text) as search_text,
    public.only_digits(search_text) as digits_search,
    least(date_from, date_to) as date_from,
    greatest(date_from, date_to) as date_to,
    greatest(coalesce(page_number, 1), 1) as page_number,
    greatest(least(coalesce(page_size, 25), 200), 1) as page_size
),
filtered as (
  select *
  from public.office_document_index i
  join params p on true
  where i.office_id = p.office_id
    and (p.company_count = 0 or i.company_id = any (p.company_ids))
    and (p.category_filter = 'todos' or i.category_key = p.category_filter)
    and (
      p.file_kind = 'todos'
      or (p.file_kind = 'xml' and i.file_extension = 'xml')
      or (p.file_kind = 'pdf' and i.file_extension = 'pdf')
    )
    and (p.date_from is null or coalesce(i.document_date, i.created_at::date) >= p.date_from)
    and (p.date_to is null or coalesce(i.document_date, i.created_at::date) <= p.date_to)
    and (
      p.search_text = ''
      or i.search_text_normalized like '%' || p.search_text || '%'
      or (p.digits_search <> '' and public.only_digits(coalesce(i.cnpj, '')) like '%' || p.digits_search || '%')
      or coalesce(i.chave, '') like '%' || btrim(coalesce(search_text, '')) || '%'
    )
),
ordered as (
  select
    source_record_id as id,
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
    count(*) over ()::int as total_count,
    row_number() over (order by coalesce(document_date, created_at::date) desc, created_at desc, source_record_id desc)::int as row_num
  from filtered
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
where row_num > (p.page_number - 1) * p.page_size
  and row_num <= p.page_number * p.page_size
order by row_num;
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
security definer
set search_path = public
as $$
with params as (
  select
    public.current_office_id() as office_id,
    upper(coalesce(detail_kind, 'NFS')) as detail_kind,
    coalesce(company_ids, '{}'::uuid[]) as company_ids,
    coalesce(array_length(company_ids, 1), 0) as company_count,
    public.normalize_search_text(search_text) as search_text,
    public.only_digits(search_text) as digits_search,
    least(date_from, date_to) as date_from,
    greatest(date_from, date_to) as date_to,
    lower(coalesce(nullif(btrim(file_kind), ''), 'all')) as file_kind,
    lower(coalesce(nullif(btrim(origem_filter), ''), 'all')) as origem_filter,
    lower(coalesce(nullif(btrim(modelo_filter), ''), 'all')) as modelo_filter,
    lower(coalesce(nullif(btrim(certidao_tipo_filter), ''), 'all')) as certidao_tipo_filter,
    greatest(coalesce(page_number, 1), 1) as page_number,
    greatest(least(coalesce(page_size, 25), 200), 1) as page_size
),
filtered as (
  select *
  from public.office_document_index i
  join params p on true
  where i.office_id = p.office_id
    and (
      (p.detail_kind = 'CERTIDOES' and i.source = 'certidoes')
      or (p.detail_kind = 'NFS' and i.source = 'fiscal' and i.type = 'NFS')
      or (p.detail_kind = 'NFE' and i.source = 'fiscal' and i.type = 'NFE')
      or (p.detail_kind = 'NFC' and i.source = 'fiscal' and i.type = 'NFC')
      or (p.detail_kind = 'NFE_NFC' and i.source = 'fiscal' and i.type in ('NFE', 'NFC'))
    )
    and (p.company_count = 0 or i.company_id = any (p.company_ids))
    and (p.date_from is null or coalesce(i.document_date, i.created_at::date) >= p.date_from)
    and (p.date_to is null or coalesce(i.document_date, i.created_at::date) <= p.date_to)
    and (
      p.search_text = ''
      or i.search_text_normalized like '%' || p.search_text || '%'
      or (p.digits_search <> '' and public.only_digits(coalesce(i.cnpj, '')) like '%' || p.digits_search || '%')
      or coalesce(i.chave, '') like '%' || btrim(coalesce(search_text, '')) || '%'
    )
    and (
      p.file_kind = 'all'
      or (p.file_kind = 'xml' and i.file_extension = 'xml')
      or (p.file_kind = 'pdf' and i.file_extension = 'pdf')
    )
    and (
      p.origem_filter = 'all'
      or p.detail_kind <> 'NFS'
      or coalesce(i.origem, '') = p.origem_filter
    )
    and (
      p.modelo_filter = 'all'
      or p.detail_kind <> 'NFE_NFC'
      or coalesce(i.modelo, '') = p.modelo_filter
    )
    and (
      p.certidao_tipo_filter = 'all'
      or p.detail_kind <> 'CERTIDOES'
      or coalesce(i.tipo_certidao, '') = p.certidao_tipo_filter
    )
),
ordered as (
  select
    source_record_id as id,
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
    count(*) over ()::int as total_count,
    row_number() over (order by coalesce(document_date, created_at::date) desc, created_at desc, source_record_id desc)::int as row_num
  from filtered
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
where row_num > (p.page_number - 1) * p.page_size
  and row_num <= p.page_number * p.page_size
order by row_num;
$$;

revoke all on function public.get_document_rows_cursor(uuid[], text, text, text, date, date, date, timestamptz, uuid, integer) from public;
revoke all on function public.get_fiscal_detail_documents_cursor(text, uuid[], text, date, date, text, text, text, text, date, timestamptz, uuid, integer) from public;
revoke all on function public.get_document_rows_page(uuid[], text, text, text, date, date, integer, integer) from public;
revoke all on function public.get_fiscal_detail_documents_page(text, uuid[], text, date, date, text, text, text, text, integer, integer) from public;
revoke all on function public.get_certidoes_overview_summary(uuid[]) from public;
revoke all on function public.get_fiscal_detail_summary(text, uuid[], date, date) from public;
revoke all on function public.get_ir_overview_summary(text) from public;
revoke all on function public.get_operations_overview_summary() from public;
revoke all on function public.get_nfs_stats_range_summary(uuid[], date, date) from public;
revoke all on function public.get_municipal_tax_debts_page(uuid[], text, text, date, date, text, text, text, integer, integer) from public;
revoke all on function public.get_tax_intelligence_overview_summary(uuid[]) from public;
revoke all on function public.get_paralegal_certificate_overview_summary(uuid[]) from public;
revoke all on function public.get_municipal_tax_overview_summary(uuid[], text, text, date, date, text) from public;
revoke all on function public.get_dashboard_overview_summary(uuid[]) from public;
revoke all on function public.get_fiscal_overview_analytics_summary(uuid[], date, date) from public;

grant execute on function public.get_document_rows_cursor(uuid[], text, text, text, date, date, date, timestamptz, uuid, integer) to authenticated;
grant execute on function public.get_fiscal_detail_documents_cursor(text, uuid[], text, date, date, text, text, text, text, date, timestamptz, uuid, integer) to authenticated;
grant execute on function public.get_document_rows_page(uuid[], text, text, text, date, date, integer, integer) to authenticated;
grant execute on function public.get_fiscal_detail_documents_page(text, uuid[], text, date, date, text, text, text, text, integer, integer) to authenticated;
grant execute on function public.get_certidoes_overview_summary(uuid[]) to authenticated;
grant execute on function public.get_fiscal_detail_summary(text, uuid[], date, date) to authenticated;
grant execute on function public.get_ir_overview_summary(text) to authenticated;
grant execute on function public.get_operations_overview_summary() to authenticated;
grant execute on function public.get_nfs_stats_range_summary(uuid[], date, date) to authenticated;
grant execute on function public.get_municipal_tax_debts_page(uuid[], text, text, date, date, text, text, text, integer, integer) to authenticated;
grant execute on function public.get_tax_intelligence_overview_summary(uuid[]) to authenticated;
grant execute on function public.get_paralegal_certificate_overview_summary(uuid[]) to authenticated;
grant execute on function public.get_municipal_tax_overview_summary(uuid[], text, text, date, date, text) to authenticated;
grant execute on function public.get_dashboard_overview_summary(uuid[]) to authenticated;
grant execute on function public.get_fiscal_overview_analytics_summary(uuid[], date, date) to authenticated;

create or replace function public.get_certidoes_overview_summary(company_ids uuid[] default null)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_office_id uuid := public.current_office_id();
  v_payload jsonb;
begin
  if coalesce(array_length(company_ids, 1), 0) = 0 then
    select summary_payload
      into v_payload
      from public.office_certificate_summary
     where office_id = v_office_id;

    if v_payload is null then
      perform public.refresh_office_document_index(v_office_id);
      perform public.refresh_office_certificate_summary(v_office_id);
      select summary_payload
        into v_payload
        from public.office_certificate_summary
       where office_id = v_office_id;
    end if;

    return coalesce(v_payload, jsonb_build_object('cards', jsonb_build_object('total', 0, 'negativas', 0, 'irregulares', 0), 'chartData', jsonb_build_array()));
  end if;

  return (
    with filtered as (
      select status
      from public.office_document_index
      where office_id = v_office_id
        and source = 'certidoes'
        and company_id = any (company_ids)
    )
    select jsonb_build_object(
      'cards',
      jsonb_build_object(
        'total', count(*)::int,
        'negativas', count(*) filter (where status = 'negativa')::int,
        'irregulares', count(*) filter (where status <> 'negativa')::int
      ),
      'chartData',
      jsonb_build_array(
        jsonb_build_object('name', 'Negativas', 'value', count(*) filter (where status = 'negativa')::int),
        jsonb_build_object('name', 'Irregulares', 'value', count(*) filter (where status <> 'negativa')::int)
      )
    )
    from filtered
  );
end;
$$;

create or replace function public.get_ir_overview_summary(responsavel_filter text default null)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_office_id uuid := public.current_office_id();
  v_payload jsonb;
begin
  if nullif(btrim(responsavel_filter), '') is null then
    select summary_payload
      into v_payload
      from public.office_ir_summary
     where office_id = v_office_id;

    if v_payload is null then
      perform public.refresh_office_ir_summary(v_office_id);
      select summary_payload
        into v_payload
        from public.office_ir_summary
       where office_id = v_office_id;
    end if;

    return coalesce(v_payload, '{}'::jsonb);
  end if;

  return (
    with rows as (
      select *
      from public.ir_clients
      where office_id = v_office_id
        and coalesce(responsavel_ir, '') = nullif(btrim(responsavel_filter), '')
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
        'concluidoPercent', case when coalesce((select total from summary), 0) = 0 then 0 else round((coalesce((select concluded_count from summary), 0)::numeric / (select total from summary)::numeric) * 100) end,
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
      'paidValuePercent', case when coalesce((select total_value from summary), 0) = 0 then 0 else round((coalesce((select paid_value from summary), 0) / (select total_value from summary)) * 100) end
    )
  );
end;
$$;

create or replace function public.get_operations_overview_summary()
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_office_id uuid := public.current_office_id();
  v_payload jsonb;
begin
  select summary_payload
    into v_payload
    from public.office_operations_summary
   where office_id = v_office_id;

  if v_payload is null then
    perform public.refresh_office_operations_summary(v_office_id);
    select summary_payload
      into v_payload
      from public.office_operations_summary
     where office_id = v_office_id;
  end if;

  return coalesce(v_payload, '{}'::jsonb);
end;
$$;

create or replace function public.get_tax_intelligence_overview_summary(company_ids uuid[] default null)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_office_id uuid := public.current_office_id();
  v_payload jsonb;
begin
  if coalesce(array_length(company_ids, 1), 0) = 0 then
    select summary_payload
      into v_payload
      from public.office_tax_intelligence_summary
     where office_id = v_office_id;

    if v_payload is null then
      perform public.refresh_office_tax_intelligence_summary(v_office_id);
      select summary_payload
        into v_payload
        from public.office_tax_intelligence_summary
       where office_id = v_office_id;
    end if;

    return coalesce(v_payload, '{}'::jsonb);
  end if;

  return (
    with rows as (
      select
        c.id,
        c.company_id,
        c.updated_at,
        coalesce(c.result_payload ->> 'appliedAnnex', '') as applied_annex,
        coalesce(c.result_payload ->> 'apurationPeriod', '') as apuration_period,
        coalesce(nullif(c.result_payload ->> 'estimatedDas', ''), '0')::numeric as estimated_das,
        coalesce(nullif(c.result_payload ->> 'effectiveRate', ''), '0')::numeric as effective_rate
      from public.simple_national_calculations c
      where c.office_id = v_office_id
        and c.company_id = any (company_ids)
    ),
    summary as (
      select
        count(*)::int as total_rows,
        count(distinct company_id)::int as empresas_ativas,
        coalesce(avg(estimated_das), 0)::numeric as media_das,
        coalesce(avg(effective_rate), 0)::numeric as media_aliquota
      from rows
    )
    select jsonb_build_object(
      'cards',
      jsonb_build_object(
        'calculosSalvos', coalesce((select total_rows from summary), 0),
        'mediaDas', coalesce((select media_das from summary), 0),
        'mediaAliquotaEfetiva', coalesce((select media_aliquota from summary), 0),
        'empresasAtivas', coalesce((select empresas_ativas from summary), 0)
      )
    )
  );
end;
$$;
