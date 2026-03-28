drop index if exists public.fiscal_documents_office_company_type_file_path_unique;

create unique index if not exists fiscal_documents_office_company_type_file_path_unique
  on public.fiscal_documents (office_id, company_id, type, file_path);
