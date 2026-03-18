create table if not exists public.tax_rule_versions (
  id uuid primary key default gen_random_uuid(),
  regime text not null,
  scope text not null,
  version_code text not null,
  effective_from date not null,
  effective_to date null,
  title text not null,
  source_reference text not null,
  source_url text null,
  payload jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (regime, scope, version_code)
);

create table if not exists public.simple_national_periods (
  id uuid primary key default gen_random_uuid(),
  company_id uuid not null references public.companies(id) on delete cascade,
  apuration_period text not null check (apuration_period ~ '^[0-9]{4}-[0-9]{2}$'),
  current_period_revenue numeric(18,2) not null default 0,
  subject_to_factor_r boolean not null default true,
  base_annex text not null default 'V' check (base_annex in ('III', 'IV', 'V')),
  activity_label text null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (company_id, apuration_period)
);

create table if not exists public.simple_national_entries (
  id uuid primary key default gen_random_uuid(),
  period_id uuid not null references public.simple_national_periods(id) on delete cascade,
  company_id uuid not null references public.companies(id) on delete cascade,
  reference_month text not null check (reference_month ~ '^[0-9]{4}-[0-9]{2}$'),
  entry_type text not null check (entry_type in ('revenue', 'payroll')),
  amount numeric(18,2) not null default 0,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (period_id, reference_month, entry_type)
);

create table if not exists public.simple_national_calculations (
  id uuid primary key default gen_random_uuid(),
  period_id uuid not null references public.simple_national_periods(id) on delete cascade,
  company_id uuid not null references public.companies(id) on delete cascade,
  rule_version_code text not null,
  result_payload jsonb not null default '{}'::jsonb,
  memory_payload jsonb not null default '[]'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (period_id)
);

create index if not exists idx_tax_rule_versions_regime_scope on public.tax_rule_versions(regime, scope);
create index if not exists idx_simple_national_periods_company_period on public.simple_national_periods(company_id, apuration_period desc);
create index if not exists idx_simple_national_entries_period_type on public.simple_national_entries(period_id, entry_type, reference_month);
create index if not exists idx_simple_national_calculations_company on public.simple_national_calculations(company_id, updated_at desc);

alter table public.tax_rule_versions enable row level security;
alter table public.simple_national_periods enable row level security;
alter table public.simple_national_entries enable row level security;
alter table public.simple_national_calculations enable row level security;

drop policy if exists "authenticated_all_tax_rule_versions" on public.tax_rule_versions;
create policy "authenticated_all_tax_rule_versions"
  on public.tax_rule_versions for all to authenticated using (true) with check (true);

drop policy if exists "authenticated_all_simple_national_periods" on public.simple_national_periods;
create policy "authenticated_all_simple_national_periods"
  on public.simple_national_periods for all to authenticated using (true) with check (true);

drop policy if exists "authenticated_all_simple_national_entries" on public.simple_national_entries;
create policy "authenticated_all_simple_national_entries"
  on public.simple_national_entries for all to authenticated using (true) with check (true);

drop policy if exists "authenticated_all_simple_national_calculations" on public.simple_national_calculations;
create policy "authenticated_all_simple_national_calculations"
  on public.simple_national_calculations for all to authenticated using (true) with check (true);
