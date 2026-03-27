create or replace function public.can_operate_office(target_office_id uuid)
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
        and om.role in ('owner', 'admin', 'operator')
    )
$$;

create table if not exists public.declaration_run_history (
  id uuid primary key default gen_random_uuid(),
  office_id uuid not null default public.current_office_id() references public.offices(id) on delete cascade,
  run_id text not null,
  action text not null,
  mode text not null,
  title text not null,
  status text not null check (status in ('processando', 'sucesso', 'divergente')),
  company_ids uuid[] not null default '{}'::uuid[],
  request_ids uuid[] not null default '{}'::uuid[],
  items_total integer not null default 0,
  items_success integer not null default 0,
  items_error integer not null default 0,
  items_processing integer not null default 0,
  payload jsonb not null default '{}'::jsonb,
  started_at timestamptz not null default now(),
  finished_at timestamptz,
  last_event_at timestamptz not null default now(),
  created_by uuid default auth.uid() references public.profiles(id) on delete set null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint declaration_run_history_office_run_key unique (office_id, run_id)
);

create index if not exists declaration_run_history_office_started_idx
  on public.declaration_run_history (office_id, started_at desc, created_at desc);

create index if not exists declaration_run_history_office_status_idx
  on public.declaration_run_history (office_id, status, started_at desc);

alter table public.declaration_run_history enable row level security;

drop trigger if exists declaration_run_history_set_updated_at on public.declaration_run_history;
create trigger declaration_run_history_set_updated_at
  before update on public.declaration_run_history
  for each row execute procedure public.set_updated_at();

drop policy if exists declaration_run_history_select on public.declaration_run_history;
create policy declaration_run_history_select on public.declaration_run_history
  for select to authenticated
  using (public.can_view_office(office_id));

drop policy if exists declaration_run_history_insert on public.declaration_run_history;
create policy declaration_run_history_insert on public.declaration_run_history
  for insert to authenticated
  with check (public.can_operate_office(office_id));

drop policy if exists declaration_run_history_update on public.declaration_run_history;
create policy declaration_run_history_update on public.declaration_run_history
  for update to authenticated
  using (public.can_operate_office(office_id))
  with check (public.can_operate_office(office_id));

drop policy if exists declaration_run_history_delete on public.declaration_run_history;
create policy declaration_run_history_delete on public.declaration_run_history
  for delete to authenticated
  using (public.can_operate_office(office_id));
