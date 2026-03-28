alter table public.fiscal_documents
  drop constraint if exists fiscal_documents_type_check;

alter table public.fiscal_documents
  add constraint fiscal_documents_type_check
  check (
    type in (
      'NFS',
      'NFE',
      'NFC',
      'GUIA_SIMPLES_DAS'
    )
  );

alter table public.fiscal_documents
  add column if not exists checksum text,
  add column if not exists parsed_at timestamptz,
  add column if not exists parser_version text,
  add column if not exists meta jsonb not null default '{}'::jsonb,
  add column if not exists amount_cents bigint,
  add column if not exists data_vencimento date;

create index if not exists fiscal_documents_office_type_company_period_idx
  on public.fiscal_documents (office_id, type, company_id, periodo desc);

create index if not exists fiscal_documents_office_type_due_date_idx
  on public.fiscal_documents (office_id, type, data_vencimento desc nulls last);

create index if not exists fiscal_documents_office_type_amount_idx
  on public.fiscal_documents (office_id, type, amount_cents desc nulls last);

create index if not exists fiscal_documents_office_type_checksum_idx
  on public.fiscal_documents (office_id, type, checksum);

create unique index if not exists fiscal_documents_office_company_type_file_path_unique
  on public.fiscal_documents (office_id, company_id, type, file_path)
  where file_path is not null;
