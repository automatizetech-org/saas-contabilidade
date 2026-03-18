alter table public.simple_national_periods
  alter column subject_to_factor_r set default false;

alter table public.simple_national_periods
  alter column base_annex set default 'I';

alter table public.simple_national_periods
  add column if not exists company_start_date date null;

alter table public.simple_national_periods
  drop constraint if exists simple_national_periods_base_annex_check;

alter table public.simple_national_periods
  add constraint simple_national_periods_base_annex_check
  check (base_annex in ('I', 'II', 'III', 'IV', 'V'));

create table if not exists public.simple_national_revenue_segments (
  id uuid primary key default gen_random_uuid(),
  period_id uuid not null references public.simple_national_periods(id) on delete cascade,
  company_id uuid not null references public.companies(id) on delete cascade,
  segment_code text not null check (segment_code in ('standard', 'annex_ii_ipi_iss')),
  market_type text not null default 'internal' check (market_type in ('internal', 'external')),
  description text null,
  amount numeric(18,2) not null default 0,
  display_order integer not null default 0,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists public.simple_national_payroll_compositions (
  id uuid primary key default gen_random_uuid(),
  period_id uuid not null unique references public.simple_national_periods(id) on delete cascade,
  company_id uuid not null references public.companies(id) on delete cascade,
  employees_amount numeric(18,2) not null default 0,
  pro_labore_amount numeric(18,2) not null default 0,
  individual_contractors_amount numeric(18,2) not null default 0,
  thirteenth_salary_amount numeric(18,2) not null default 0,
  employer_cpp_amount numeric(18,2) not null default 0,
  fgts_amount numeric(18,2) not null default 0,
  excluded_profit_distribution_amount numeric(18,2) not null default 0,
  excluded_rent_amount numeric(18,2) not null default 0,
  excluded_interns_amount numeric(18,2) not null default 0,
  excluded_mei_amount numeric(18,2) not null default 0,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists idx_simple_national_revenue_segments_period on public.simple_national_revenue_segments(period_id, display_order);
create index if not exists idx_simple_national_payroll_compositions_period on public.simple_national_payroll_compositions(period_id);

alter table public.simple_national_revenue_segments enable row level security;
alter table public.simple_national_payroll_compositions enable row level security;

drop policy if exists "authenticated_all_simple_national_revenue_segments" on public.simple_national_revenue_segments;
create policy "authenticated_all_simple_national_revenue_segments"
  on public.simple_national_revenue_segments for all to authenticated using (true) with check (true);

drop policy if exists "authenticated_all_simple_national_payroll_compositions" on public.simple_national_payroll_compositions;
create policy "authenticated_all_simple_national_payroll_compositions"
  on public.simple_national_payroll_compositions for all to authenticated using (true) with check (true);
