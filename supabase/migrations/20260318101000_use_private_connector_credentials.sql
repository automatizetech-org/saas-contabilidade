create table if not exists public.office_server_credentials (
  id uuid primary key default gen_random_uuid(),
  office_server_id uuid not null unique references public.office_servers(id) on delete cascade,
  secret_hash text not null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

alter table public.office_server_credentials enable row level security;

drop trigger if exists office_server_credentials_set_updated_at on public.office_server_credentials;
create trigger office_server_credentials_set_updated_at
  before update on public.office_server_credentials
  for each row execute procedure public.set_updated_at();

do $$
begin
  if exists (
    select 1
    from information_schema.columns
    where table_schema = 'public'
      and table_name = 'office_servers'
      and column_name = 'server_secret_hash'
  ) then
    insert into public.office_server_credentials (office_server_id, secret_hash)
    select os.id, os.server_secret_hash
    from public.office_servers os
    where os.server_secret_hash is not null
      and not exists (
        select 1
        from public.office_server_credentials osc
        where osc.office_server_id = os.id
      );
  end if;
end $$;

alter table public.office_servers
  drop column if exists server_secret_hash;
