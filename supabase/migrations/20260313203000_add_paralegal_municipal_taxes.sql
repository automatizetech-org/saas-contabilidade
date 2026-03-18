create table if not exists public.municipal_tax_debts (
  id uuid primary key default gen_random_uuid(),
  company_id uuid not null references public.companies(id) on delete cascade,
  ano integer,
  tributo text not null,
  numero_documento text,
  data_vencimento date,
  valor numeric(14,2) not null default 0,
  situacao text,
  portal_inscricao text,
  portal_cai text,
  detalhes jsonb not null default '{}'::jsonb,
  fetched_at timestamptz not null default now(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint municipal_tax_debts_dedupe_unique unique (company_id, tributo, numero_documento, data_vencimento)
);

create index if not exists municipal_tax_debts_company_idx
  on public.municipal_tax_debts (company_id);

create index if not exists municipal_tax_debts_due_idx
  on public.municipal_tax_debts (data_vencimento);

create index if not exists municipal_tax_debts_status_idx
  on public.municipal_tax_debts (situacao);

create table if not exists public.municipal_tax_collection_runs (
  id uuid primary key default gen_random_uuid(),
  robot_technical_id text not null,
  company_id uuid references public.companies(id) on delete cascade,
  company_name text,
  status text not null check (status in ('pending', 'running', 'completed', 'failed')),
  started_at timestamptz,
  finished_at timestamptz,
  debts_found integer not null default 0,
  error_message text,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists municipal_tax_collection_runs_robot_idx
  on public.municipal_tax_collection_runs (robot_technical_id, created_at desc);

create index if not exists municipal_tax_collection_runs_company_idx
  on public.municipal_tax_collection_runs (company_id, created_at desc);

insert into public.robots (
  technical_id,
  display_name,
  status,
  segment_path,
  is_fiscal_notes_robot,
  fiscal_notes_kind,
  notes_mode,
  global_logins
)
select
  'goiania_taxas_impostos',
  'Taxas e Impostos Goiânia',
  'inactive',
  'PARALEGAL/TAXAS-IMPOSTOS',
  false,
  null,
  null,
  '[]'::jsonb
where not exists (
  select 1
  from public.robots
  where technical_id = 'goiania_taxas_impostos'
);
