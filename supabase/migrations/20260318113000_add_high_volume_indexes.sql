create index if not exists fiscal_documents_office_created_idx on public.fiscal_documents (office_id, created_at desc);
create index if not exists fiscal_documents_office_document_date_idx on public.fiscal_documents (office_id, document_date desc);
create index if not exists fiscal_documents_office_period_type_idx on public.fiscal_documents (office_id, periodo, type);
create index if not exists fiscal_documents_office_status_created_idx on public.fiscal_documents (office_id, status, created_at desc);

create index if not exists fiscal_pendencias_office_status_created_idx on public.fiscal_pendencias (office_id, status, created_at desc);
create index if not exists fiscal_pendencias_office_company_period_idx on public.fiscal_pendencias (office_id, company_id, periodo);

create index if not exists dp_checklist_office_status_competencia_idx on public.dp_checklist (office_id, status, competencia desc);
create index if not exists dp_checklist_office_company_competencia_idx on public.dp_checklist (office_id, company_id, competencia desc);

create index if not exists dp_guias_office_data_idx on public.dp_guias (office_id, data desc);
create index if not exists dp_guias_office_company_created_idx on public.dp_guias (office_id, company_id, created_at desc);

create index if not exists financial_records_office_period_idx on public.financial_records (office_id, periodo desc);
create index if not exists financial_records_office_status_created_idx on public.financial_records (office_id, status, created_at desc);
create index if not exists financial_records_office_company_period_idx on public.financial_records (office_id, company_id, periodo desc);

create index if not exists nfs_stats_office_period_idx on public.nfs_stats (office_id, period desc);

create index if not exists sync_events_office_created_idx on public.sync_events (office_id, created_at desc);
create index if not exists sync_events_office_tipo_created_idx on public.sync_events (office_id, tipo, created_at desc);
