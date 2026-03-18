alter table public.simple_national_periods
  add column if not exists municipal_iss_rate numeric(5,2) null;
