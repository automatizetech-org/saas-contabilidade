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
  server_secret_hash text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create unique index if not exists office_servers_one_active_per_office on public.office_servers (office_id) where is_active = true;

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

create table if not exists public.fiscal_pendencias (
  id uuid primary key default gen_random_uuid(),
  office_id uuid not null default public.current_office_id() references public.offices(id) on delete cascade,
  company_id uuid not null references public.companies(id) on delete cascade,
  tipo text not null check (tipo in ('NFS', 'NFE', 'NFC')),
  periodo text not null,
  status public.document_status not null,
  created_at timestamptz not null default now()
);

create table if not exists public.dp_checklist (
  id uuid primary key default gen_random_uuid(),
  office_id uuid not null default public.current_office_id() references public.offices(id) on delete cascade,
  company_id uuid not null references public.companies(id) on delete cascade,
  tarefa text not null,
  competencia text not null,
  status public.document_status not null default 'pendente',
  created_at timestamptz not null default now()
);

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
    'profiles','offices','office_memberships','office_servers','office_branding','robots',
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
