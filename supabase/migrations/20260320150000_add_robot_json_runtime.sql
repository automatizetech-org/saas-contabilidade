alter table public.office_servers
  add column if not exists robots_root_path text not null default 'C:\Users\ROBO\Documents\ROBOS';

alter table public.robots
  add column if not exists runtime_folder text,
  add column if not exists entrypoint_relpath text not null default 'bot.py',
  add column if not exists job_file_relpath text not null default 'data/json/job.json',
  add column if not exists result_file_relpath text not null default 'data/json/result.json',
  add column if not exists heartbeat_file_relpath text not null default 'data/json/heartbeat.json',
  add column if not exists capabilities jsonb not null default '{}'::jsonb,
  add column if not exists runtime_defaults jsonb not null default '{}'::jsonb,
  add column if not exists admin_form_schema jsonb not null default '[]'::jsonb,
  add column if not exists company_form_schema jsonb not null default '[]'::jsonb,
  add column if not exists schedule_form_schema jsonb not null default '[]'::jsonb;

update public.robots
set runtime_folder = coalesce(nullif(btrim(runtime_folder), ''), technical_id)
where runtime_folder is null or nullif(btrim(runtime_folder), '') is null;

create table if not exists public.office_robot_configs (
  office_id uuid not null default public.current_office_id() references public.offices(id) on delete cascade,
  robot_technical_id text not null references public.robots(technical_id) on delete cascade,
  display_name text,
  segment_path text,
  notes_mode text,
  date_execution_mode text,
  initial_period_start date,
  initial_period_end date,
  last_period_end date,
  global_logins jsonb not null default '[]'::jsonb,
  admin_settings jsonb not null default '{}'::jsonb,
  execution_defaults jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (office_id, robot_technical_id)
);

insert into public.office_robot_configs (
  office_id,
  robot_technical_id,
  display_name,
  segment_path,
  notes_mode,
  date_execution_mode,
  initial_period_start,
  initial_period_end,
  last_period_end,
  global_logins
)
select
  o.id,
  r.technical_id,
  r.display_name,
  r.segment_path,
  r.notes_mode,
  r.date_execution_mode,
  r.initial_period_start,
  r.initial_period_end,
  r.last_period_end,
  coalesce(r.global_logins, '[]'::jsonb)
from public.offices o
cross join public.robots r
on conflict (office_id, robot_technical_id) do update
set
  display_name = excluded.display_name,
  segment_path = excluded.segment_path,
  notes_mode = excluded.notes_mode,
  date_execution_mode = excluded.date_execution_mode,
  initial_period_start = excluded.initial_period_start,
  initial_period_end = excluded.initial_period_end,
  last_period_end = excluded.last_period_end,
  global_logins = excluded.global_logins,
  updated_at = now();

create table if not exists public.office_robot_runtime (
  office_id uuid not null references public.offices(id) on delete cascade,
  office_server_id uuid not null references public.office_servers(id) on delete cascade,
  robot_technical_id text not null references public.robots(technical_id) on delete cascade,
  status text not null default 'inactive' check (status in ('active', 'inactive', 'processing')),
  last_heartbeat_at timestamptz,
  current_execution_request_id uuid references public.execution_requests(id) on delete set null,
  current_job_id text,
  runtime_version text,
  host_name text,
  heartbeat_payload jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (office_server_id, robot_technical_id)
);

insert into public.office_robot_runtime (
  office_id,
  office_server_id,
  robot_technical_id,
  status,
  last_heartbeat_at,
  current_job_id,
  heartbeat_payload
)
select
  os.office_id,
  os.id,
  r.technical_id,
  r.status,
  r.last_heartbeat_at,
  null,
  jsonb_build_object(
    'status', r.status,
    'last_heartbeat_at', r.last_heartbeat_at
  )
from public.office_servers os
cross join public.robots r
where os.is_active = true
on conflict (office_server_id, robot_technical_id) do update
set
  status = excluded.status,
  last_heartbeat_at = excluded.last_heartbeat_at,
  heartbeat_payload = excluded.heartbeat_payload,
  updated_at = now();

alter table public.company_robot_config
  add column if not exists settings jsonb not null default '{}'::jsonb;

update public.company_robot_config
set settings = jsonb_strip_nulls(
  coalesce(settings, '{}'::jsonb) ||
  jsonb_build_object(
    'auth_mode', auth_mode,
    'nfs_password', nfs_password,
    'selected_login_cpf', selected_login_cpf
  )
);

alter table public.schedule_rules
  add column if not exists settings jsonb not null default '{}'::jsonb;

alter table public.execution_requests
  add column if not exists job_payload jsonb not null default '{}'::jsonb,
  add column if not exists result_summary jsonb not null default '{}'::jsonb,
  add column if not exists source text not null default 'manual',
  add column if not exists claimed_by_server_id uuid references public.office_servers(id) on delete set null,
  add column if not exists started_at timestamptz;

create table if not exists public.robot_result_events (
  id uuid primary key default gen_random_uuid(),
  event_id uuid not null unique default gen_random_uuid(),
  office_id uuid not null references public.offices(id) on delete cascade,
  execution_request_id uuid references public.execution_requests(id) on delete set null,
  robot_technical_id text not null references public.robots(technical_id) on delete cascade,
  status text not null check (status in ('completed', 'failed')),
  summary jsonb not null default '{}'::jsonb,
  company_results jsonb not null default '[]'::jsonb,
  payload jsonb not null default '{}'::jsonb,
  error_message text,
  created_at timestamptz not null default now(),
  ingested_at timestamptz not null default now()
);

create index if not exists office_robot_runtime_office_idx
  on public.office_robot_runtime (office_id, status, updated_at desc);

create index if not exists robot_result_events_office_idx
  on public.robot_result_events (office_id, created_at desc);

drop function if exists public.get_visible_robots();

drop policy if exists office_robot_configs_select on public.office_robot_configs;
create policy office_robot_configs_select on public.office_robot_configs
  for select to authenticated
  using (public.can_view_office(office_id));

drop policy if exists office_robot_configs_write on public.office_robot_configs;
create policy office_robot_configs_write on public.office_robot_configs
  for all to authenticated
  using (public.can_manage_office(office_id))
  with check (public.can_manage_office(office_id));

drop policy if exists office_robot_runtime_select on public.office_robot_runtime;
create policy office_robot_runtime_select on public.office_robot_runtime
  for select to authenticated
  using (public.can_view_office(office_id));

drop policy if exists robot_result_events_select on public.robot_result_events;
create policy robot_result_events_select on public.robot_result_events
  for select to authenticated
  using (public.can_view_office(office_id));

create or replace function public.get_visible_robots()
returns table (
  id uuid,
  technical_id text,
  display_name text,
  status text,
  last_heartbeat_at timestamptz,
  segment_path text,
  created_at timestamptz,
  updated_at timestamptz,
  notes_mode text,
  date_execution_mode text,
  initial_period_start date,
  initial_period_end date,
  last_period_end date,
  is_fiscal_notes_robot boolean,
  fiscal_notes_kind text,
  global_logins jsonb,
  runtime_folder text,
  entrypoint_relpath text,
  job_file_relpath text,
  result_file_relpath text,
  heartbeat_file_relpath text,
  capabilities jsonb,
  runtime_defaults jsonb,
  admin_form_schema jsonb,
  company_form_schema jsonb,
  schedule_form_schema jsonb,
  admin_settings jsonb,
  execution_defaults jsonb
)
language sql
stable
security definer
set search_path = public
as $$
  with viewer as (
    select
      auth.uid() as user_id,
      public.current_office_id() as office_id,
      public.is_platform_super_admin() as is_super_admin
  )
  select
    r.id,
    r.technical_id,
    coalesce(nullif(btrim(cfg.display_name), ''), r.display_name) as display_name,
    coalesce(rt.status, r.status, 'inactive') as status,
    coalesce(rt.last_heartbeat_at, r.last_heartbeat_at) as last_heartbeat_at,
    coalesce(nullif(btrim(cfg.segment_path), ''), r.segment_path) as segment_path,
    r.created_at,
    greatest(r.updated_at, coalesce(cfg.updated_at, r.updated_at), coalesce(rt.updated_at, r.updated_at)) as updated_at,
    coalesce(cfg.notes_mode, r.notes_mode) as notes_mode,
    coalesce(cfg.date_execution_mode, r.date_execution_mode) as date_execution_mode,
    coalesce(cfg.initial_period_start, r.initial_period_start) as initial_period_start,
    coalesce(cfg.initial_period_end, r.initial_period_end) as initial_period_end,
    coalesce(cfg.last_period_end, r.last_period_end) as last_period_end,
    r.is_fiscal_notes_robot,
    r.fiscal_notes_kind,
    case
      when v.is_super_admin then coalesce(cfg.global_logins, r.global_logins, '[]'::jsonb)
      else coalesce((
        select jsonb_agg(
          jsonb_build_object(
            'cpf', public.only_digits(coalesce(item ->> 'cpf', item ->> 'login', item ->> 'username', '')),
            'password', case
              when nullif(btrim(coalesce(item ->> 'password', item ->> 'senha', '')), '') is not null then '__configured__'
              else null
            end,
            'is_default', coalesce((item ->> 'is_default')::boolean, false)
          )
        )
        from jsonb_array_elements(coalesce(cfg.global_logins, r.global_logins, '[]'::jsonb)) as item
        where length(public.only_digits(coalesce(item ->> 'cpf', item ->> 'login', item ->> 'username', ''))) = 11
      ), '[]'::jsonb)
    end as global_logins,
    coalesce(nullif(btrim(r.runtime_folder), ''), r.technical_id) as runtime_folder,
    r.entrypoint_relpath,
    r.job_file_relpath,
    r.result_file_relpath,
    r.heartbeat_file_relpath,
    coalesce(r.capabilities, '{}'::jsonb) as capabilities,
    coalesce(r.runtime_defaults, '{}'::jsonb) as runtime_defaults,
    coalesce(r.admin_form_schema, '[]'::jsonb) as admin_form_schema,
    coalesce(r.company_form_schema, '[]'::jsonb) as company_form_schema,
    coalesce(r.schedule_form_schema, '[]'::jsonb) as schedule_form_schema,
    coalesce(cfg.admin_settings, '{}'::jsonb) as admin_settings,
    coalesce(cfg.execution_defaults, '{}'::jsonb) as execution_defaults
  from public.robots r
  cross join viewer v
  left join public.office_robot_configs cfg
    on cfg.office_id = v.office_id
   and cfg.robot_technical_id = r.technical_id
  left join public.office_servers os
    on os.office_id = v.office_id
   and os.is_active = true
  left join public.office_robot_runtime rt
    on rt.office_server_id = os.id
   and rt.robot_technical_id = r.technical_id
  where v.user_id is not null
  order by coalesce(nullif(btrim(cfg.display_name), ''), r.display_name) asc
$$;
