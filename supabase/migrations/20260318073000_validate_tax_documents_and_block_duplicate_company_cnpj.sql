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
  check (public.is_valid_cpf(cpf)) not valid;

alter table public.companies
  drop constraint if exists companies_document_valid_check;
alter table public.companies
  add constraint companies_document_valid_check
  check (document is null or public.is_valid_cnpj(document)) not valid;

alter table public.companies
  drop constraint if exists companies_contador_cpf_valid_check;
alter table public.companies
  add constraint companies_contador_cpf_valid_check
  check (contador_cpf is null or public.is_valid_cpf(contador_cpf)) not valid;

alter table public.companies
  drop constraint if exists companies_sefaz_go_logins_valid_check;
alter table public.companies
  add constraint companies_sefaz_go_logins_valid_check
  check (public.are_valid_portal_logins(sefaz_go_logins)) not valid;

alter table public.company_robot_config
  drop constraint if exists company_robot_config_selected_login_cpf_valid_check;
alter table public.company_robot_config
  add constraint company_robot_config_selected_login_cpf_valid_check
  check (selected_login_cpf is null or public.is_valid_cpf(selected_login_cpf)) not valid;

alter table public.robots
  drop constraint if exists robots_global_logins_valid_check;
alter table public.robots
  add constraint robots_global_logins_valid_check
  check (public.are_valid_portal_logins(global_logins)) not valid;

alter table public.ir_clients
  drop constraint if exists ir_clients_cpf_cnpj_valid_check;
alter table public.ir_clients
  add constraint ir_clients_cpf_cnpj_valid_check
  check (public.is_valid_cpf_or_cnpj(cpf_cnpj)) not valid;

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
