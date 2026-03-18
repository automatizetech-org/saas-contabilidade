alter table public.simple_national_periods
  alter column subject_to_factor_r set default false;

alter table public.simple_national_periods
  alter column base_annex set default 'I';

alter table public.simple_national_periods
  drop constraint if exists simple_national_periods_base_annex_check;

alter table public.simple_national_periods
  add constraint simple_national_periods_base_annex_check
  check (base_annex in ('I', 'II', 'III', 'IV', 'V'));
