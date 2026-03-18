alter table public.companies
  add column if not exists state_code text,
  add column if not exists city_name text;

create index if not exists companies_state_city_idx
  on public.companies (state_code, city_name);
