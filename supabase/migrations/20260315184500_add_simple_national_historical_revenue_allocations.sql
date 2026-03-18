create table if not exists public.simple_national_historical_revenue_allocations (
  id uuid primary key default gen_random_uuid(),
  period_id uuid not null references public.simple_national_periods(id) on delete cascade,
  company_id uuid not null references public.companies(id) on delete cascade,
  reference_month text not null check (reference_month ~ '^[0-9]{4}-[0-9]{2}$'),
  annex_code text not null check (annex_code in ('I', 'II', 'III', 'IV', 'V')),
  amount numeric(18,2) not null default 0,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists idx_simple_national_historical_revenue_allocations_period
  on public.simple_national_historical_revenue_allocations(period_id, reference_month, annex_code);

alter table public.simple_national_historical_revenue_allocations enable row level security;

drop policy if exists "authenticated_all_simple_national_historical_revenue_allocations"
  on public.simple_national_historical_revenue_allocations;

create policy "authenticated_all_simple_national_historical_revenue_allocations"
  on public.simple_national_historical_revenue_allocations
  for all
  to authenticated
  using (true)
  with check (true);
