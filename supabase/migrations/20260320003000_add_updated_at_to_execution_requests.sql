alter table public.execution_requests
  add column if not exists updated_at timestamptz not null default timezone('utc', now());

update public.execution_requests
set updated_at = coalesce(updated_at, created_at, timezone('utc', now()))
where updated_at is null;
