-- =============================================================================
-- SCHEMA COMPLETO SUPABASE - ESTRUTURA EM PRODUÇÃO
-- Consolidado a partir de DATABASE.SQL + migrations (tabelas, RLS, triggers, storage).
-- Uso: referência e documentação. Para aplicar em novo ambiente use as migrations.
-- =============================================================================

-- Tipo usado em documentos/checklist (status)
DO $$ BEGIN
  CREATE TYPE public.document_status AS ENUM (
    'novo', 'pendente', 'concluido', 'processando', 'enviado', 'erro', 'baixado'
  );
EXCEPTION
  WHEN duplicate_object THEN NULL;
END $$;

-- -----------------------------------------------------------------------------
-- TABELAS (ordem por dependência)
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS public.profiles (
  id uuid NOT NULL,
  username text NOT NULL UNIQUE,
  role text NOT NULL CHECK (role = ANY (ARRAY['super_admin'::text, 'user'::text])),
  panel_access jsonb NOT NULL DEFAULT '{"dp": true, "sync": true, "fiscal": true, "empresas": true, "dashboard": true, "operacoes": true, "documentos": true, "financeiro": true}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT profiles_pkey PRIMARY KEY (id),
  CONSTRAINT profiles_id_fkey FOREIGN KEY (id) REFERENCES auth.users(id)
);

CREATE TABLE IF NOT EXISTS public.companies (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  name text NOT NULL,
  document text,
  active boolean NOT NULL DEFAULT true,
  created_by uuid,
  created_at timestamptz NOT NULL DEFAULT now(),
  auth_mode text CHECK (auth_mode IS NULL OR (auth_mode = ANY (ARRAY['password'::text, 'certificate'::text]))),
  cert_blob_b64 text,
  cert_password text,
  cert_valid_until date,
  contador_nome text,
  contador_cpf text,
  state_registration text,
  sefaz_go_logins jsonb NOT NULL DEFAULT '[]'::jsonb,
  state_code text,
  city_name text,
  cae text,
  CONSTRAINT companies_pkey PRIMARY KEY (id),
  CONSTRAINT companies_created_by_fkey FOREIGN KEY (created_by) REFERENCES public.profiles(id)
);

CREATE TABLE IF NOT EXISTS public.accountants (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  name text NOT NULL,
  cpf text NOT NULL,
  active boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT accountants_pkey PRIMARY KEY (id)
);
CREATE UNIQUE INDEX IF NOT EXISTS accountants_cpf_key ON public.accountants (cpf);

CREATE TABLE IF NOT EXISTS public.admin_settings (
  key text NOT NULL,
  value text NOT NULL DEFAULT '',
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT admin_settings_pkey PRIMARY KEY (key)
);
-- RLS: anon SELECT (robô Goiânia lê robot_goiania_skip_iss); authenticated ALL (dashboard).
ALTER TABLE public.admin_settings ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "admin_settings_anon_select" ON public.admin_settings;
CREATE POLICY "admin_settings_anon_select" ON public.admin_settings FOR SELECT TO anon USING (true);
DROP POLICY IF EXISTS "admin_settings_authenticated_all" ON public.admin_settings;
CREATE POLICY "admin_settings_authenticated_all" ON public.admin_settings FOR ALL TO authenticated USING (true) WITH CHECK (true);

CREATE TABLE IF NOT EXISTS public.client_branding_settings (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  client_id text NOT NULL DEFAULT 'default' UNIQUE,
  primary_color text,
  secondary_color text,
  tertiary_color text,
  logo_url text,
  favicon_url text,
  use_custom_palette boolean NOT NULL DEFAULT false,
  use_custom_logo boolean NOT NULL DEFAULT false,
  use_custom_favicon boolean NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  client_name text,
  CONSTRAINT client_branding_settings_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.company_memberships (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  company_id uuid NOT NULL REFERENCES public.companies(id),
  user_id uuid NOT NULL REFERENCES public.profiles(id),
  member_role text NOT NULL DEFAULT 'member' CHECK (member_role IN ('owner', 'admin', 'member')),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT company_memberships_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.company_robot_config (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  company_id uuid NOT NULL REFERENCES public.companies(id),
  robot_technical_id text NOT NULL,
  enabled boolean NOT NULL DEFAULT true,
  auth_mode text NOT NULL DEFAULT 'password' CHECK (auth_mode IN ('password', 'certificate')),
  nfs_password text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  selected_login_cpf text,
  CONSTRAINT company_robot_config_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.robots (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  technical_id text NOT NULL UNIQUE,
  display_name text NOT NULL,
  status text NOT NULL DEFAULT 'inactive' CHECK (status IN ('active', 'inactive', 'processing')),
  last_heartbeat_at timestamptz,
  segment_path text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  notes_mode text,
  date_execution_mode text,
  initial_period_start date,
  initial_period_end date,
  last_period_end date,
  is_fiscal_notes_robot boolean NOT NULL DEFAULT false,
  fiscal_notes_kind text,
  global_logins jsonb NOT NULL DEFAULT '[]'::jsonb,
  CONSTRAINT robots_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.schedule_rules (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  company_ids uuid[] NOT NULL DEFAULT '{}',
  robot_technical_ids text[] NOT NULL DEFAULT '{}',
  notes_mode text,
  period_start date,
  period_end date,
  run_at_time time NOT NULL,
  run_daily boolean NOT NULL DEFAULT true,
  status text NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'paused', 'completed')),
  last_run_at timestamptz,
  created_by uuid REFERENCES public.profiles(id),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  run_at_date date,
  execution_mode text NOT NULL DEFAULT 'sequential',
  CONSTRAINT schedule_rules_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.execution_requests (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  company_ids uuid[] NOT NULL DEFAULT '{}',
  robot_technical_ids text[] NOT NULL DEFAULT '{}',
  status text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'running', 'completed', 'failed')),
  robot_id uuid REFERENCES public.robots(id),
  claimed_at timestamptz,
  completed_at timestamptz,
  error_message text,
  period_start date,
  period_end date,
  created_at timestamptz NOT NULL DEFAULT now(),
  created_by uuid REFERENCES public.profiles(id),
  notes_mode text,
  schedule_rule_id uuid REFERENCES public.schedule_rules(id),
  execution_mode text NOT NULL DEFAULT 'sequential',
  execution_group_id uuid,
  execution_order integer,
  CONSTRAINT execution_requests_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.documents (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  company_id uuid NOT NULL REFERENCES public.companies(id),
  tipo text NOT NULL CHECK (tipo IN ('NFS', 'NFE', 'NFC')),
  periodo text NOT NULL,
  status public.document_status NOT NULL DEFAULT 'novo',
  origem text NOT NULL DEFAULT 'Automação',
  document_date date,
  arquivos text[] DEFAULT '{}',
  created_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT documents_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.fiscal_documents (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  company_id uuid NOT NULL REFERENCES public.companies(id),
  type text NOT NULL CHECK (type IN ('NFS', 'NFE', 'NFC')),
  chave text,
  periodo text NOT NULL,
  status public.document_status NOT NULL DEFAULT 'novo',
  document_date date,
  file_path text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  last_downloaded_at timestamptz,
  CONSTRAINT fiscal_documents_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.fiscal_pendencias (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  company_id uuid NOT NULL REFERENCES public.companies(id),
  tipo text NOT NULL CHECK (tipo IN ('NFS', 'NFE', 'NFC')),
  periodo text NOT NULL,
  status public.document_status NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT fiscal_pendencias_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.dp_checklist (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  company_id uuid NOT NULL REFERENCES public.companies(id),
  tarefa text NOT NULL,
  competencia text NOT NULL,
  status public.document_status NOT NULL DEFAULT 'pendente',
  created_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT dp_checklist_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.dp_guias (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  company_id uuid NOT NULL REFERENCES public.companies(id),
  nome text NOT NULL,
  tipo text NOT NULL DEFAULT 'PDF',
  data date NOT NULL,
  file_path text,
  created_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT dp_guias_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.financial_records (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  company_id uuid NOT NULL REFERENCES public.companies(id),
  periodo text NOT NULL,
  valor_cents bigint NOT NULL DEFAULT 0,
  status public.document_status NOT NULL DEFAULT 'pendente',
  pendencias_count integer NOT NULL DEFAULT 0,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT financial_records_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.folder_structure_nodes (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  parent_id uuid REFERENCES public.folder_structure_nodes(id),
  name text NOT NULL,
  slug text,
  date_rule text CHECK (date_rule IN ('year', 'year_month', 'year_month_day')),
  position integer NOT NULL DEFAULT 0,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT folder_structure_nodes_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.robot_display_config (
  robot_technical_id text NOT NULL PRIMARY KEY,
  company_ids uuid[] NOT NULL DEFAULT '{}',
  period_start date,
  period_end date,
  notes_mode text,
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.automation_data (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  company_id uuid NOT NULL REFERENCES public.companies(id),
  automation_id text NOT NULL,
  date date NOT NULL,
  count_1 bigint,
  count_2 bigint,
  count_3 bigint,
  amount_1 numeric,
  metadata jsonb NOT NULL DEFAULT '{}',
  created_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT automation_data_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.ir_settings (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  singleton boolean NOT NULL DEFAULT true UNIQUE,
  payment_due_date date,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT ir_settings_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.ir_clients (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  nome text NOT NULL,
  cpf_cnpj text NOT NULL,
  valor_servico numeric NOT NULL DEFAULT 0,
  status_pagamento text NOT NULL DEFAULT 'A PAGAR' CHECK (status_pagamento IN ('PIX', 'DINHEIRO', 'TRANSFERÊNCIA POUPANÇA', 'PERMUTA', 'A PAGAR')),
  status_declaracao text NOT NULL DEFAULT 'Pendente' CHECK (status_declaracao IN ('Pendente', 'Concluido')),
  observacoes text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  responsavel_ir text,
  vencimento date,
  payment_charge_type text,
  payment_charge_status text NOT NULL DEFAULT 'none' CHECK (payment_charge_status IN ('none', 'pending', 'paid', 'failed', 'cancelled')),
  payment_charge_id text,
  payment_charge_correlation_id text,
  payment_provider text,
  payment_link text,
  payment_pix_copy_paste text,
  payment_pix_qr_code text,
  payment_boleto_pdf_base64 text,
  payment_boleto_barcode text,
  payment_boleto_digitable_line text,
  payment_paid_at timestamptz,
  payment_payer_name text,
  payment_payer_tax_id text,
  payment_generated_at timestamptz,
  payment_last_webhook_at timestamptz,
  payment_metadata jsonb NOT NULL DEFAULT '{}',
  CONSTRAINT ir_clients_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.municipal_tax_collection_runs (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  robot_technical_id text NOT NULL,
  company_id uuid REFERENCES public.companies(id),
  company_name text,
  status text NOT NULL CHECK (status IN ('pending', 'running', 'completed', 'failed')),
  started_at timestamptz,
  finished_at timestamptz,
  debts_found integer NOT NULL DEFAULT 0,
  error_message text,
  metadata jsonb NOT NULL DEFAULT '{}',
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT municipal_tax_collection_runs_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.municipal_tax_debts (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  company_id uuid NOT NULL REFERENCES public.companies(id),
  ano integer,
  tributo text NOT NULL,
  numero_documento text,
  data_vencimento date,
  valor numeric NOT NULL DEFAULT 0,
  situacao text,
  portal_inscricao text,
  portal_cai text,
  detalhes jsonb NOT NULL DEFAULT '{}',
  fetched_at timestamptz NOT NULL DEFAULT now(),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  guia_pdf_path text,
  CONSTRAINT municipal_tax_debts_pkey PRIMARY KEY (id),
  CONSTRAINT municipal_tax_debts_dedupe_unique UNIQUE (company_id, tributo, numero_documento, data_vencimento)
);

CREATE TABLE IF NOT EXISTS public.nfs_stats (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  company_id uuid NOT NULL REFERENCES public.companies(id),
  period text NOT NULL CHECK (period ~ '^\d{4}-\d{2}$'),
  qty_emitidas integer NOT NULL DEFAULT 0,
  qty_recebidas integer NOT NULL DEFAULT 0,
  valor_emitidas numeric NOT NULL DEFAULT 0,
  valor_recebidas numeric NOT NULL DEFAULT 0,
  service_codes jsonb NOT NULL DEFAULT '[]',
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  service_codes_emitidas jsonb NOT NULL DEFAULT '[]',
  service_codes_recebidas jsonb NOT NULL DEFAULT '[]',
  CONSTRAINT nfs_stats_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.sync_events (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  company_id uuid REFERENCES public.companies(id),
  tipo text NOT NULL,
  payload text,
  status public.document_status NOT NULL,
  idempotency_key text,
  retries integer NOT NULL DEFAULT 0,
  created_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT sync_events_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.tax_rule_versions (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  regime text NOT NULL,
  scope text NOT NULL,
  version_code text NOT NULL,
  effective_from date NOT NULL,
  effective_to date,
  title text NOT NULL,
  source_reference text NOT NULL,
  source_url text,
  payload jsonb NOT NULL DEFAULT '{}',
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT tax_rule_versions_pkey PRIMARY KEY (id),
  CONSTRAINT tax_rule_versions_regime_scope_version_code_key UNIQUE (regime, scope, version_code)
);

CREATE TABLE IF NOT EXISTS public.simple_national_periods (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  company_id uuid NOT NULL REFERENCES public.companies(id),
  apuration_period text NOT NULL CHECK (apuration_period ~ '^[0-9]{4}-[0-9]{2}$'),
  company_start_date date,
  current_period_revenue numeric NOT NULL DEFAULT 0,
  subject_to_factor_r boolean NOT NULL DEFAULT false,
  base_annex text NOT NULL DEFAULT 'I' CHECK (base_annex IN ('I', 'II', 'III', 'IV', 'V')),
  activity_label text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  municipal_iss_rate numeric,
  CONSTRAINT simple_national_periods_pkey PRIMARY KEY (id),
  CONSTRAINT simple_national_periods_company_id_apuration_period_key UNIQUE (company_id, apuration_period)
);

CREATE TABLE IF NOT EXISTS public.simple_national_entries (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  period_id uuid NOT NULL REFERENCES public.simple_national_periods(id),
  company_id uuid NOT NULL REFERENCES public.companies(id),
  reference_month text NOT NULL CHECK (reference_month ~ '^[0-9]{4}-[0-9]{2}$'),
  entry_type text NOT NULL CHECK (entry_type IN ('revenue', 'payroll')),
  amount numeric NOT NULL DEFAULT 0,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT simple_national_entries_pkey PRIMARY KEY (id),
  CONSTRAINT simple_national_entries_period_ref_type_key UNIQUE (period_id, reference_month, entry_type)
);

CREATE TABLE IF NOT EXISTS public.simple_national_calculations (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  period_id uuid NOT NULL UNIQUE REFERENCES public.simple_national_periods(id),
  company_id uuid NOT NULL REFERENCES public.companies(id),
  rule_version_code text NOT NULL,
  result_payload jsonb NOT NULL DEFAULT '{}',
  memory_payload jsonb NOT NULL DEFAULT '[]',
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT simple_national_calculations_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.simple_national_historical_revenue_allocations (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  period_id uuid NOT NULL REFERENCES public.simple_national_periods(id),
  company_id uuid NOT NULL REFERENCES public.companies(id),
  reference_month text NOT NULL CHECK (reference_month ~ '^[0-9]{4}-[0-9]{2}$'),
  annex_code text NOT NULL CHECK (annex_code IN ('I', 'II', 'III', 'IV', 'V')),
  amount numeric NOT NULL DEFAULT 0,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT simple_national_historical_revenue_allocations_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.simple_national_payroll_compositions (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  period_id uuid NOT NULL UNIQUE REFERENCES public.simple_national_periods(id),
  company_id uuid NOT NULL REFERENCES public.companies(id),
  employees_amount numeric NOT NULL DEFAULT 0,
  pro_labore_amount numeric NOT NULL DEFAULT 0,
  individual_contractors_amount numeric NOT NULL DEFAULT 0,
  thirteenth_salary_amount numeric NOT NULL DEFAULT 0,
  employer_cpp_amount numeric NOT NULL DEFAULT 0,
  fgts_amount numeric NOT NULL DEFAULT 0,
  excluded_profit_distribution_amount numeric NOT NULL DEFAULT 0,
  excluded_rent_amount numeric NOT NULL DEFAULT 0,
  excluded_interns_amount numeric NOT NULL DEFAULT 0,
  excluded_mei_amount numeric NOT NULL DEFAULT 0,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT simple_national_payroll_compositions_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.simple_national_revenue_segments (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  period_id uuid NOT NULL REFERENCES public.simple_national_periods(id),
  company_id uuid NOT NULL REFERENCES public.companies(id),
  segment_code text NOT NULL CHECK (segment_code IN ('standard', 'annex_ii_ipi_iss', 'I', 'II', 'III', 'IV', 'V')),
  market_type text NOT NULL DEFAULT 'internal' CHECK (market_type IN ('internal', 'external')),
  description text,
  amount numeric NOT NULL DEFAULT 0,
  display_order integer NOT NULL DEFAULT 0,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT simple_national_revenue_segments_pkey PRIMARY KEY (id)
);

-- -----------------------------------------------------------------------------
-- ÍNDICES EXTRAS
-- -----------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_tax_rule_versions_regime_scope ON public.tax_rule_versions(regime, scope);
CREATE INDEX IF NOT EXISTS idx_simple_national_periods_company_period ON public.simple_national_periods(company_id, apuration_period DESC);
CREATE INDEX IF NOT EXISTS idx_simple_national_entries_period_type ON public.simple_national_entries(period_id, entry_type, reference_month);
CREATE INDEX IF NOT EXISTS idx_simple_national_calculations_company ON public.simple_national_calculations(company_id, updated_at DESC);

CREATE INDEX IF NOT EXISTS municipal_tax_debts_company_idx ON public.municipal_tax_debts(company_id);
CREATE INDEX IF NOT EXISTS municipal_tax_debts_due_idx ON public.municipal_tax_debts(data_vencimento);
CREATE INDEX IF NOT EXISTS municipal_tax_debts_status_idx ON public.municipal_tax_debts(situacao);
CREATE INDEX IF NOT EXISTS municipal_tax_collection_runs_robot_idx ON public.municipal_tax_collection_runs(robot_technical_id, created_at DESC);
CREATE INDEX IF NOT EXISTS municipal_tax_collection_runs_company_idx ON public.municipal_tax_collection_runs(company_id, created_at DESC);

-- -----------------------------------------------------------------------------
-- FUNÇÕES E TRIGGERS
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.set_client_branding_updated_at()
RETURNS trigger AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS client_branding_settings_updated_at ON public.client_branding_settings;
CREATE TRIGGER client_branding_settings_updated_at
  BEFORE UPDATE ON public.client_branding_settings
  FOR EACH ROW EXECUTE PROCEDURE public.set_client_branding_updated_at();

-- -----------------------------------------------------------------------------
-- RLS E POLÍTICAS
-- -----------------------------------------------------------------------------

-- accountants
ALTER TABLE public.accountants ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "accountants_authenticated_all" ON public.accountants;
CREATE POLICY "accountants_authenticated_all"
  ON public.accountants FOR ALL TO authenticated USING (true) WITH CHECK (true);
DROP POLICY IF EXISTS "accountants_anon_select" ON public.accountants;
CREATE POLICY "accountants_anon_select"
  ON public.accountants FOR SELECT TO anon USING (true);

-- client_branding_settings
ALTER TABLE public.client_branding_settings ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "client_branding_select" ON public.client_branding_settings;
CREATE POLICY "client_branding_select" ON public.client_branding_settings FOR SELECT TO anon, authenticated USING (true);
DROP POLICY IF EXISTS "client_branding_insert" ON public.client_branding_settings;
CREATE POLICY "client_branding_insert" ON public.client_branding_settings FOR INSERT TO authenticated WITH CHECK (true);
DROP POLICY IF EXISTS "client_branding_update" ON public.client_branding_settings;
CREATE POLICY "client_branding_update" ON public.client_branding_settings FOR UPDATE TO authenticated USING (true) WITH CHECK (true);
DROP POLICY IF EXISTS "client_branding_delete" ON public.client_branding_settings;
CREATE POLICY "client_branding_delete" ON public.client_branding_settings FOR DELETE TO authenticated USING (true);

-- municipal_tax_*
ALTER TABLE public.municipal_tax_collection_runs ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.municipal_tax_debts ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "anon_all_municipal_tax_collection_runs" ON public.municipal_tax_collection_runs;
CREATE POLICY "anon_all_municipal_tax_collection_runs" ON public.municipal_tax_collection_runs FOR ALL TO anon USING (true) WITH CHECK (true);
DROP POLICY IF EXISTS "anon_all_municipal_tax_debts" ON public.municipal_tax_debts;
CREATE POLICY "anon_all_municipal_tax_debts" ON public.municipal_tax_debts FOR ALL TO anon USING (true) WITH CHECK (true);
DROP POLICY IF EXISTS "authenticated_select_municipal_tax_collection_runs" ON public.municipal_tax_collection_runs;
CREATE POLICY "authenticated_select_municipal_tax_collection_runs" ON public.municipal_tax_collection_runs FOR SELECT TO authenticated USING (true);
DROP POLICY IF EXISTS "authenticated_select_municipal_tax_debts" ON public.municipal_tax_debts;
CREATE POLICY "authenticated_select_municipal_tax_debts" ON public.municipal_tax_debts FOR SELECT TO authenticated USING (true);

-- tax_rule_versions e simple_national_*
ALTER TABLE public.tax_rule_versions ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.simple_national_periods ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.simple_national_entries ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.simple_national_calculations ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.simple_national_historical_revenue_allocations ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.simple_national_payroll_compositions ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.simple_national_revenue_segments ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "authenticated_all_tax_rule_versions" ON public.tax_rule_versions;
CREATE POLICY "authenticated_all_tax_rule_versions" ON public.tax_rule_versions FOR ALL TO authenticated USING (true) WITH CHECK (true);
DROP POLICY IF EXISTS "authenticated_all_simple_national_periods" ON public.simple_national_periods;
CREATE POLICY "authenticated_all_simple_national_periods" ON public.simple_national_periods FOR ALL TO authenticated USING (true) WITH CHECK (true);
DROP POLICY IF EXISTS "authenticated_all_simple_national_entries" ON public.simple_national_entries;
CREATE POLICY "authenticated_all_simple_national_entries" ON public.simple_national_entries FOR ALL TO authenticated USING (true) WITH CHECK (true);
DROP POLICY IF EXISTS "authenticated_all_simple_national_calculations" ON public.simple_national_calculations;
CREATE POLICY "authenticated_all_simple_national_calculations" ON public.simple_national_calculations FOR ALL TO authenticated USING (true) WITH CHECK (true);

-- -----------------------------------------------------------------------------
-- STORAGE (bucket branding-assets)
-- Executar apenas se o bucket ainda não existir.
-- -----------------------------------------------------------------------------
INSERT INTO storage.buckets (id, name, public, file_size_limit, allowed_mime_types)
VALUES (
  'branding-assets',
  'branding-assets',
  true,
  2097152,
  ARRAY['image/png', 'image/svg+xml', 'image/jpeg', 'image/webp', 'image/x-icon']::text[]
)
ON CONFLICT (id) DO UPDATE SET
  public = EXCLUDED.public,
  file_size_limit = EXCLUDED.file_size_limit,
  allowed_mime_types = EXCLUDED.allowed_mime_types;

DROP POLICY IF EXISTS "branding_assets_public_read" ON storage.objects;
CREATE POLICY "branding_assets_public_read" ON storage.objects FOR SELECT TO public USING (bucket_id = 'branding-assets');
DROP POLICY IF EXISTS "branding_assets_authenticated_upload" ON storage.objects;
CREATE POLICY "branding_assets_authenticated_upload" ON storage.objects FOR INSERT TO authenticated WITH CHECK (bucket_id = 'branding-assets');
DROP POLICY IF EXISTS "branding_assets_authenticated_update" ON storage.objects;
CREATE POLICY "branding_assets_authenticated_update" ON storage.objects FOR UPDATE TO authenticated USING (bucket_id = 'branding-assets');
DROP POLICY IF EXISTS "branding_assets_authenticated_delete" ON storage.objects;
CREATE POLICY "branding_assets_authenticated_delete" ON storage.objects FOR DELETE TO authenticated USING (bucket_id = 'branding-assets');
