alter table public.simple_national_revenue_segments
  drop constraint if exists simple_national_revenue_segments_segment_code_check;

alter table public.simple_national_revenue_segments
  add constraint simple_national_revenue_segments_segment_code_check
  check (segment_code in ('standard', 'annex_ii_ipi_iss', 'I', 'II', 'III', 'IV', 'V'));
