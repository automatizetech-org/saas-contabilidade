alter table public.company_robot_config
  add column if not exists settings jsonb not null default '{}'::jsonb;

create or replace function public.get_default_company_robot_auth_mode(
  company_auth_mode text,
  company_cert_blob_b64 text,
  company_cert_password text,
  robot_capabilities jsonb default '{}'::jsonb,
  robot_company_form_schema jsonb default '[]'::jsonb
)
returns text
language plpgsql
stable
security definer
set search_path = public
as $$
declare
  auth_behavior text := lower(btrim(coalesce(robot_capabilities ->> 'auth_behavior', '')));
  explicit_default_auth text := lower(
    btrim(
      coalesce(
        robot_capabilities ->> 'default_auth_mode',
        robot_capabilities ->> 'preferred_auth_mode',
        ''
      )
    )
  );
  company_prefers_certificate boolean := lower(btrim(coalesce(company_auth_mode, ''))) = 'certificate';
  company_has_certificate boolean := nullif(btrim(coalesce(company_cert_blob_b64, '')), '') is not null
    and nullif(btrim(coalesce(company_cert_password, '')), '') is not null;
  robot_supports_company_auth_choice boolean := auth_behavior = 'choice'
    or explicit_default_auth = 'certificate'
    or exists (
      select 1
      from jsonb_array_elements(coalesce(robot_company_form_schema, '[]'::jsonb)) as field
      where lower(btrim(coalesce(field ->> 'type', ''))) = 'auth_mode'
         or lower(btrim(coalesce(field ->> 'key', ''))) = 'auth_mode'
    );
begin
  if robot_supports_company_auth_choice and (company_prefers_certificate or company_has_certificate) then
    return 'certificate';
  end if;
  return 'password';
end;
$$;

create or replace function public.sync_company_robot_config_defaults(
  target_company_id uuid default null,
  target_robot_technical_id text default null
)
returns void
language plpgsql
security definer
set search_path = public
as $$
begin
  insert into public.company_robot_config (
    office_id,
    company_id,
    robot_technical_id,
    enabled,
    auth_mode,
    settings
  )
  select
    c.office_id,
    c.id,
    r.technical_id,
    true,
    defaults.default_auth_mode,
    jsonb_build_object('auth_mode', defaults.default_auth_mode)
  from public.companies c
  cross join public.robots r
  cross join lateral (
    select public.get_default_company_robot_auth_mode(
      c.auth_mode,
      c.cert_blob_b64,
      c.cert_password,
      coalesce(r.capabilities, '{}'::jsonb),
      coalesce(r.company_form_schema, '[]'::jsonb)
    ) as default_auth_mode
  ) as defaults
  where (target_company_id is null or c.id = target_company_id)
    and (target_robot_technical_id is null or r.technical_id = target_robot_technical_id)
  on conflict (company_id, robot_technical_id) do nothing;

  update public.company_robot_config crc
  set
    enabled = true,
    auth_mode = case
      when crc.auth_mode = 'password'
        and nullif(btrim(coalesce(crc.nfs_password, '')), '') is null
        and nullif(btrim(coalesce(coalesce(crc.settings, '{}'::jsonb) ->> 'nfs_password', '')), '') is null
        and defaults.default_auth_mode = 'certificate'
      then defaults.default_auth_mode
      else crc.auth_mode
    end,
    settings = case
      when crc.auth_mode = 'password'
        and nullif(btrim(coalesce(crc.nfs_password, '')), '') is null
        and nullif(btrim(coalesce(coalesce(crc.settings, '{}'::jsonb) ->> 'nfs_password', '')), '') is null
        and defaults.default_auth_mode = 'certificate'
      then jsonb_set(coalesce(crc.settings, '{}'::jsonb), '{auth_mode}', to_jsonb(defaults.default_auth_mode), true)
      when coalesce(crc.settings, '{}'::jsonb) ? 'auth_mode'
      then coalesce(crc.settings, '{}'::jsonb)
      else jsonb_set(coalesce(crc.settings, '{}'::jsonb), '{auth_mode}', to_jsonb(crc.auth_mode), true)
    end,
    updated_at = now()
  from public.companies c
  cross join public.robots r
  cross join lateral (
    select public.get_default_company_robot_auth_mode(
      c.auth_mode,
      c.cert_blob_b64,
      c.cert_password,
      coalesce(r.capabilities, '{}'::jsonb),
      coalesce(r.company_form_schema, '[]'::jsonb)
    ) as default_auth_mode
  ) as defaults
  where crc.company_id = c.id
    and crc.office_id = c.office_id
    and r.technical_id = crc.robot_technical_id
    and (target_company_id is null or c.id = target_company_id)
    and (target_robot_technical_id is null or r.technical_id = target_robot_technical_id)
    and (
      crc.enabled is distinct from true
      or not (coalesce(crc.settings, '{}'::jsonb) ? 'auth_mode')
      or (
        crc.auth_mode = 'password'
        and nullif(btrim(coalesce(crc.nfs_password, '')), '') is null
        and nullif(btrim(coalesce(coalesce(crc.settings, '{}'::jsonb) ->> 'nfs_password', '')), '') is null
        and defaults.default_auth_mode = 'certificate'
      )
    );
end;
$$;

create or replace function public.sync_company_robot_config_defaults_on_company_change()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
begin
  perform public.sync_company_robot_config_defaults(new.id, null);
  return new;
end;
$$;

create or replace function public.sync_company_robot_config_defaults_on_robot_change()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
begin
  perform public.sync_company_robot_config_defaults(null, new.technical_id);
  return new;
end;
$$;

drop trigger if exists companies_sync_company_robot_config_defaults on public.companies;
create trigger companies_sync_company_robot_config_defaults
  after insert or update of auth_mode, cert_blob_b64, cert_password on public.companies
  for each row execute procedure public.sync_company_robot_config_defaults_on_company_change();

drop trigger if exists robots_sync_company_robot_config_defaults on public.robots;
create trigger robots_sync_company_robot_config_defaults
  after insert or update of capabilities, company_form_schema on public.robots
  for each row execute procedure public.sync_company_robot_config_defaults_on_robot_change();

select public.sync_company_robot_config_defaults();
