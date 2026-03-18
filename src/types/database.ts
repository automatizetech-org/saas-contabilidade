export type Json =
  | string
  | number
  | boolean
  | null
  | { [key: string]: Json | undefined }
  | Json[]

export type RobotNotesMode =
  | "recebidas"
  | "emitidas"
  | "both"
  | "modelo_55"
  | "modelo_65"
  | "modelos_55_65"

export type FiscalNotesKind = "nfs" | "nfe_nfc"
export type RobotExecutionMode = "sequential" | "parallel"

export type Database = {
  __InternalSupabase: {
    PostgrestVersion: "14.1"
  }
  public: {
    Tables: {
      profiles: {
        Row: { id: string; username: string; role: string; created_at: string; panel_access: Record<string, boolean> }
        Insert: { id: string; username: string; role: string; created_at?: string; panel_access?: Record<string, boolean> }
        Update: { id?: string; username?: string; role?: string; created_at?: string; panel_access?: Record<string, boolean> }
      }
      companies: {
        Row: { id: string; name: string; document: string | null; created_by: string | null; created_at: string; active: boolean; auth_mode: string | null; cert_blob_b64: string | null; cert_password: string | null; cert_valid_until: string | null; contador_nome: string | null; contador_cpf: string | null; state_registration: string | null; state_code: string | null; city_name: string | null; cae: string | null; sefaz_go_logins: Json }
        Insert: { id?: string; name: string; document?: string | null; created_by?: string | null; created_at?: string; active?: boolean; auth_mode?: string | null; cert_blob_b64?: string | null; cert_password?: string | null; cert_valid_until?: string | null; contador_nome?: string | null; contador_cpf?: string | null; state_registration?: string | null; state_code?: string | null; city_name?: string | null; cae?: string | null; sefaz_go_logins?: Json }
        Update: { id?: string; name?: string; document?: string | null; created_by?: string | null; created_at?: string; active?: boolean; auth_mode?: string | null; cert_blob_b64?: string | null; cert_password?: string | null; cert_valid_until?: string | null; contador_nome?: string | null; contador_cpf?: string | null; state_registration?: string | null; state_code?: string | null; city_name?: string | null; cae?: string | null; sefaz_go_logins?: Json }
      }
      accountants: {
        Row: { id: string; name: string; cpf: string; active: boolean; created_at: string; updated_at: string }
        Insert: { id?: string; name: string; cpf: string; active?: boolean; created_at?: string; updated_at?: string }
        Update: { id?: string; name?: string; cpf?: string; active?: boolean; created_at?: string; updated_at?: string }
      }
      fiscal_documents: {
        Row: { id: string; company_id: string; type: string; chave: string | null; periodo: string; status: string; document_date: string | null; file_path: string | null; last_downloaded_at: string | null; created_at: string; updated_at: string }
        Insert: { id?: string; company_id: string; type: string; chave?: string | null; periodo: string; status?: string; document_date?: string | null; file_path?: string | null; last_downloaded_at?: string | null; created_at?: string; updated_at?: string }
        Update: { id?: string; company_id?: string; type?: string; chave?: string | null; periodo?: string; status?: string; document_date?: string | null; file_path?: string | null; last_downloaded_at?: string | null; created_at?: string; updated_at?: string }
      }
      folder_structure_nodes: {
        Row: { id: string; parent_id: string | null; name: string; slug: string | null; date_rule: "year" | "year_month" | "year_month_day" | null; position: number; created_at: string; updated_at: string }
        Insert: { id?: string; parent_id?: string | null; name: string; slug?: string | null; date_rule?: "year" | "year_month" | "year_month_day" | null; position?: number; created_at?: string; updated_at?: string }
        Update: { id?: string; parent_id?: string | null; name?: string; slug?: string | null; date_rule?: "year" | "year_month" | "year_month_day" | null; position?: number; created_at?: string; updated_at?: string }
      }
      robots: {
        Row: { id: string; technical_id: string; display_name: string; status: "active" | "inactive" | "processing"; last_heartbeat_at: string | null; segment_path: string | null; is_fiscal_notes_robot: boolean; fiscal_notes_kind: FiscalNotesKind | null; notes_mode: RobotNotesMode | null; date_execution_mode: "competencia" | "interval" | null; initial_period_start: string | null; initial_period_end: string | null; last_period_end: string | null; global_logins: Json; created_at: string; updated_at: string }
        Insert: { id?: string; technical_id: string; display_name: string; status?: "active" | "inactive" | "processing"; last_heartbeat_at?: string | null; segment_path?: string | null; is_fiscal_notes_robot?: boolean; fiscal_notes_kind?: FiscalNotesKind | null; notes_mode?: RobotNotesMode | null; date_execution_mode?: "competencia" | "interval" | null; initial_period_start?: string | null; initial_period_end?: string | null; last_period_end?: string | null; global_logins?: Json; created_at?: string; updated_at?: string }
        Update: { id?: string; technical_id?: string; display_name?: string; status?: "active" | "inactive" | "processing"; last_heartbeat_at?: string | null; segment_path?: string | null; is_fiscal_notes_robot?: boolean; fiscal_notes_kind?: FiscalNotesKind | null; notes_mode?: RobotNotesMode | null; date_execution_mode?: "competencia" | "interval" | null; initial_period_start?: string | null; initial_period_end?: string | null; last_period_end?: string | null; global_logins?: Json; created_at?: string; updated_at?: string }
      }
      execution_requests: {
        Row: { id: string; company_ids: string[]; robot_technical_ids: string[]; status: "pending" | "running" | "completed" | "failed"; robot_id: string | null; claimed_at: string | null; completed_at: string | null; error_message: string | null; period_start: string | null; period_end: string | null; notes_mode: RobotNotesMode | null; schedule_rule_id: string | null; execution_mode: RobotExecutionMode | null; execution_group_id: string | null; execution_order: number | null; created_at: string; created_by: string | null }
        Insert: { id?: string; company_ids: string[]; robot_technical_ids: string[]; status?: "pending" | "running" | "completed" | "failed"; robot_id?: string | null; claimed_at?: string | null; completed_at?: string | null; error_message?: string | null; period_start?: string | null; period_end?: string | null; notes_mode?: RobotNotesMode | null; schedule_rule_id?: string | null; execution_mode?: RobotExecutionMode | null; execution_group_id?: string | null; execution_order?: number | null; created_at?: string; created_by?: string | null }
        Update: { id?: string; company_ids?: string[]; robot_technical_ids?: string[]; status?: "pending" | "running" | "completed" | "failed"; robot_id?: string | null; claimed_at?: string | null; completed_at?: string | null; error_message?: string | null; period_start?: string | null; period_end?: string | null; notes_mode?: RobotNotesMode | null; schedule_rule_id?: string | null; execution_mode?: RobotExecutionMode | null; execution_group_id?: string | null; execution_order?: number | null; created_at?: string; created_by?: string | null }
      }
      schedule_rules: {
        Row: { id: string; company_ids: string[]; robot_technical_ids: string[]; notes_mode: RobotNotesMode | null; period_start: string | null; period_end: string | null; run_at_date: string | null; run_at_time: string; run_daily: boolean; execution_mode: RobotExecutionMode | null; status: "active" | "paused" | "completed"; last_run_at: string | null; created_by: string | null; created_at: string; updated_at: string }
        Insert: { id?: string; company_ids: string[]; robot_technical_ids: string[]; notes_mode?: RobotNotesMode | null; period_start?: string | null; period_end?: string | null; run_at_date?: string | null; run_at_time: string; run_daily?: boolean; execution_mode?: RobotExecutionMode | null; status?: "active" | "paused" | "completed"; last_run_at?: string | null; created_by?: string | null; created_at?: string; updated_at?: string }
        Update: { id?: string; company_ids?: string[]; robot_technical_ids?: string[]; notes_mode?: RobotNotesMode | null; period_start?: string | null; period_end?: string | null; run_at_date?: string | null; run_at_time?: string; run_daily?: boolean; execution_mode?: RobotExecutionMode | null; status?: "active" | "paused" | "completed"; last_run_at?: string | null; created_by?: string | null; created_at?: string; updated_at?: string }
      }
      robot_display_config: {
        Row: { robot_technical_id: string; company_ids: string[]; period_start: string | null; period_end: string | null; notes_mode: RobotNotesMode | null; updated_at: string }
        Insert: { robot_technical_id: string; company_ids?: string[]; period_start?: string | null; period_end?: string | null; notes_mode?: RobotNotesMode | null; updated_at?: string }
        Update: { robot_technical_id?: string; company_ids?: string[]; period_start?: string | null; period_end?: string | null; notes_mode?: RobotNotesMode | null; updated_at?: string }
      }
      company_robot_config: {
        Row: { id: string; company_id: string; robot_technical_id: string; enabled: boolean; auth_mode: "password" | "certificate"; nfs_password: string | null; selected_login_cpf: string | null; created_at: string; updated_at: string }
        Insert: { id?: string; company_id: string; robot_technical_id: string; enabled?: boolean; auth_mode?: "password" | "certificate"; nfs_password?: string | null; selected_login_cpf?: string | null; created_at?: string; updated_at?: string }
        Update: { id?: string; company_id?: string; robot_technical_id?: string; enabled?: boolean; auth_mode?: "password" | "certificate"; nfs_password?: string | null; selected_login_cpf?: string | null; created_at?: string; updated_at?: string }
      }
      fiscal_pendencias: { Row: { id: string; company_id: string; tipo: string; periodo: string; status: string; created_at: string }; Insert: { id?: string; company_id: string; tipo: string; periodo: string; status: string; created_at?: string }; Update: Partial<{ id: string; company_id: string; tipo: string; periodo: string; status: string; created_at: string }> }
      dp_checklist: { Row: { id: string; company_id: string; tarefa: string; competencia: string; status: string; created_at: string }; Insert: { id?: string; company_id: string; tarefa: string; competencia: string; status?: string; created_at?: string }; Update: Partial<{ id: string; company_id: string; tarefa: string; competencia: string; status: string; created_at: string }> }
      dp_guias: { Row: { id: string; company_id: string; nome: string; tipo: string; data: string; file_path: string | null; created_at: string }; Insert: { id?: string; company_id: string; nome: string; tipo?: string; data: string; file_path?: string | null; created_at?: string }; Update: Partial<{ id: string; company_id: string; nome: string; tipo: string; data: string; file_path: string | null; created_at: string }> }
      financial_records: { Row: { id: string; company_id: string; periodo: string; valor_cents: number; status: string; pendencias_count: number; created_at: string; updated_at: string }; Insert: { id?: string; company_id: string; periodo: string; valor_cents?: number; status?: string; pendencias_count?: number; created_at?: string; updated_at?: string }; Update: Partial<{ id: string; company_id: string; periodo: string; valor_cents: number; status: string; pendencias_count: number; created_at: string; updated_at: string }> }
      sync_events: { Row: { id: string; company_id: string | null; tipo: string; payload: string | null; status: string; idempotency_key: string | null; retries: number; created_at: string }; Insert: { id?: string; company_id?: string | null; tipo: string; payload?: string | null; status: string; idempotency_key?: string | null; retries?: number; created_at?: string }; Update: Partial<{ id: string; company_id: string | null; tipo: string; payload: string | null; status: string; idempotency_key: string | null; retries: number; created_at: string }> }
      documents: { Row: { id: string; company_id: string; tipo: string; periodo: string; status: string; origem: string; document_date: string | null; arquivos: string[]; created_at: string }; Insert: { id?: string; company_id: string; tipo: string; periodo: string; status?: string; origem?: string; document_date?: string | null; arquivos?: string[]; created_at?: string }; Update: Partial<{ id: string; company_id: string; tipo: string; periodo: string; status: string; origem: string; document_date: string | null; arquivos: string[]; created_at: string }> }
      admin_settings: {
        Row: { key: string; value: string; updated_at: string }
        Insert: { key: string; value?: string; updated_at?: string }
        Update: { key?: string; value?: string; updated_at?: string }
      }
      client_branding_settings: {
        Row: { id: string; client_id: string; client_name: string | null; primary_color: string | null; secondary_color: string | null; tertiary_color: string | null; logo_url: string | null; favicon_url: string | null; use_custom_palette: boolean; use_custom_logo: boolean; use_custom_favicon: boolean; created_at: string; updated_at: string }
        Insert: { id?: string; client_id?: string; client_name?: string | null; primary_color?: string | null; secondary_color?: string | null; tertiary_color?: string | null; logo_url?: string | null; favicon_url?: string | null; use_custom_palette?: boolean; use_custom_logo?: boolean; use_custom_favicon?: boolean; created_at?: string; updated_at?: string }
        Update: { id?: string; client_id?: string; client_name?: string | null; primary_color?: string | null; secondary_color?: string | null; tertiary_color?: string | null; logo_url?: string | null; favicon_url?: string | null; use_custom_palette?: boolean; use_custom_logo?: boolean; use_custom_favicon?: boolean; created_at?: string; updated_at?: string }
      }
      municipal_tax_debts: {
        Row: { id: string; company_id: string; ano: number | null; tributo: string; numero_documento: string | null; data_vencimento: string | null; valor: number; situacao: string | null; portal_inscricao: string | null; portal_cai: string | null; detalhes: Json; fetched_at: string; created_at: string; updated_at: string; guia_pdf_path: string | null }
        Insert: { id?: string; company_id: string; ano?: number | null; tributo: string; numero_documento?: string | null; data_vencimento?: string | null; valor?: number; situacao?: string | null; portal_inscricao?: string | null; portal_cai?: string | null; detalhes?: Json; fetched_at?: string; created_at?: string; updated_at?: string; guia_pdf_path?: string | null }
        Update: { id?: string; company_id?: string; ano?: number | null; tributo?: string; numero_documento?: string | null; data_vencimento?: string | null; valor?: number; situacao?: string | null; portal_inscricao?: string | null; portal_cai?: string | null; detalhes?: Json; fetched_at?: string; created_at?: string; updated_at?: string; guia_pdf_path?: string | null }
      }
      municipal_tax_collection_runs: {
        Row: { id: string; robot_technical_id: string; company_id: string | null; company_name: string | null; status: "pending" | "running" | "completed" | "failed"; started_at: string | null; finished_at: string | null; debts_found: number; error_message: string | null; metadata: Json; created_at: string; updated_at: string }
        Insert: { id?: string; robot_technical_id: string; company_id?: string | null; company_name?: string | null; status: "pending" | "running" | "completed" | "failed"; started_at?: string | null; finished_at?: string | null; debts_found?: number; error_message?: string | null; metadata?: Json; created_at?: string; updated_at?: string }
        Update: { id?: string; robot_technical_id?: string; company_id?: string | null; company_name?: string | null; status?: "pending" | "running" | "completed" | "failed"; started_at?: string | null; finished_at?: string | null; debts_found?: number; error_message?: string | null; metadata?: Json; created_at?: string; updated_at?: string }
      }
      ir_clients: {
        Row: {
          id: string
          nome: string
          cpf_cnpj: string
          responsavel_ir: string | null
          vencimento: string | null
          valor_servico: number
          status_pagamento: "PIX" | "DINHEIRO" | "TRANSFERÊNCIA POUPANÇA" | "PERMUTA" | "A PAGAR"
          status_declaracao: "Concluido" | "Pendente"
          observacoes: string | null
          created_at: string
          updated_at: string
          payment_charge_type: "PIX" | "BOLETO" | "BOLETO_HIBRIDO" | null
          payment_charge_status: "none" | "pending" | "paid" | "failed" | "cancelled"
          payment_charge_id: string | null
          payment_charge_correlation_id: string | null
          payment_provider: string | null
          payment_link: string | null
          payment_pix_copy_paste: string | null
          payment_pix_qr_code: string | null
          payment_boleto_pdf_base64: string | null
          payment_boleto_barcode: string | null
          payment_boleto_digitable_line: string | null
          payment_paid_at: string | null
          payment_payer_name: string | null
          payment_payer_tax_id: string | null
          payment_generated_at: string | null
          payment_last_webhook_at: string | null
          payment_metadata: Json
        }
        Insert: {
          id?: string
          nome: string
          cpf_cnpj: string
          responsavel_ir?: string | null
          vencimento?: string | null
          valor_servico?: number
          status_pagamento?: "PIX" | "DINHEIRO" | "TRANSFERÊNCIA POUPANÇA" | "PERMUTA" | "A PAGAR"
          status_declaracao?: "Concluido" | "Pendente"
          observacoes?: string | null
          created_at?: string
          updated_at?: string
          payment_charge_type?: "PIX" | "BOLETO" | "BOLETO_HIBRIDO" | null
          payment_charge_status?: "none" | "pending" | "paid" | "failed" | "cancelled"
          payment_charge_id?: string | null
          payment_charge_correlation_id?: string | null
          payment_provider?: string | null
          payment_link?: string | null
          payment_pix_copy_paste?: string | null
          payment_pix_qr_code?: string | null
          payment_boleto_pdf_base64?: string | null
          payment_boleto_barcode?: string | null
          payment_boleto_digitable_line?: string | null
          payment_paid_at?: string | null
          payment_payer_name?: string | null
          payment_payer_tax_id?: string | null
          payment_generated_at?: string | null
          payment_last_webhook_at?: string | null
          payment_metadata?: Json
        }
        Update: {
          id?: string
          nome?: string
          cpf_cnpj?: string
          responsavel_ir?: string | null
          vencimento?: string | null
          valor_servico?: number
          status_pagamento?: "PIX" | "DINHEIRO" | "TRANSFERÊNCIA POUPANÇA" | "PERMUTA" | "A PAGAR"
          status_declaracao?: "Concluido" | "Pendente"
          observacoes?: string | null
          created_at?: string
          updated_at?: string
          payment_charge_type?: "PIX" | "BOLETO" | "BOLETO_HIBRIDO" | null
          payment_charge_status?: "none" | "pending" | "paid" | "failed" | "cancelled"
          payment_charge_id?: string | null
          payment_charge_correlation_id?: string | null
          payment_provider?: string | null
          payment_link?: string | null
          payment_pix_copy_paste?: string | null
          payment_pix_qr_code?: string | null
          payment_boleto_pdf_base64?: string | null
          payment_boleto_barcode?: string | null
          payment_boleto_digitable_line?: string | null
          payment_paid_at?: string | null
          payment_payer_name?: string | null
          payment_payer_tax_id?: string | null
          payment_generated_at?: string | null
          payment_last_webhook_at?: string | null
          payment_metadata?: Json
        }
      }
      ir_settings: {
        Row: { id: string; singleton: boolean; payment_due_date: string | null; created_at: string; updated_at: string }
        Insert: { id?: string; singleton?: boolean; payment_due_date?: string | null; created_at?: string; updated_at?: string }
        Update: { id?: string; singleton?: boolean; payment_due_date?: string | null; created_at?: string; updated_at?: string }
      }
      tax_rule_versions: {
        Row: { id: string; regime: string; scope: string; version_code: string; effective_from: string; effective_to: string | null; title: string; source_reference: string; source_url: string | null; payload: Json; created_at: string; updated_at: string }
        Insert: { id?: string; regime: string; scope: string; version_code: string; effective_from: string; effective_to?: string | null; title: string; source_reference: string; source_url?: string | null; payload?: Json; created_at?: string; updated_at?: string }
        Update: { id?: string; regime?: string; scope?: string; version_code?: string; effective_from?: string; effective_to?: string | null; title?: string; source_reference?: string; source_url?: string | null; payload?: Json; created_at?: string; updated_at?: string }
      }
      simple_national_periods: {
        Row: { id: string; company_id: string; apuration_period: string; company_start_date: string | null; current_period_revenue: number; municipal_iss_rate: number | null; subject_to_factor_r: boolean; base_annex: string; activity_label: string | null; created_at: string; updated_at: string }
        Insert: { id?: string; company_id: string; apuration_period: string; company_start_date?: string | null; current_period_revenue?: number; municipal_iss_rate?: number | null; subject_to_factor_r?: boolean; base_annex?: string; activity_label?: string | null; created_at?: string; updated_at?: string }
        Update: { id?: string; company_id?: string; apuration_period?: string; company_start_date?: string | null; current_period_revenue?: number; municipal_iss_rate?: number | null; subject_to_factor_r?: boolean; base_annex?: string; activity_label?: string | null; created_at?: string; updated_at?: string }
      }
      simple_national_entries: {
        Row: { id: string; period_id: string; company_id: string; reference_month: string; entry_type: "revenue" | "payroll"; amount: number; created_at: string; updated_at: string }
        Insert: { id?: string; period_id: string; company_id: string; reference_month: string; entry_type: "revenue" | "payroll"; amount?: number; created_at?: string; updated_at?: string }
        Update: { id?: string; period_id?: string; company_id?: string; reference_month?: string; entry_type?: "revenue" | "payroll"; amount?: number; created_at?: string; updated_at?: string }
      }
      simple_national_historical_revenue_allocations: {
        Row: { id: string; period_id: string; company_id: string; reference_month: string; annex_code: string; amount: number; created_at: string; updated_at: string }
        Insert: { id?: string; period_id: string; company_id: string; reference_month: string; annex_code: string; amount?: number; created_at?: string; updated_at?: string }
        Update: { id?: string; period_id?: string; company_id?: string; reference_month?: string; annex_code?: string; amount?: number; created_at?: string; updated_at?: string }
      }
      simple_national_revenue_segments: {
        Row: { id: string; period_id: string; company_id: string; segment_code: string; market_type: string; description: string | null; amount: number; display_order: number; created_at: string; updated_at: string }
        Insert: { id?: string; period_id: string; company_id: string; segment_code: string; market_type?: string; description?: string | null; amount?: number; display_order?: number; created_at?: string; updated_at?: string }
        Update: { id?: string; period_id?: string; company_id?: string; segment_code?: string; market_type?: string; description?: string | null; amount?: number; display_order?: number; created_at?: string; updated_at?: string }
      }
      simple_national_payroll_compositions: {
        Row: { id: string; period_id: string; company_id: string; employees_amount: number; pro_labore_amount: number; individual_contractors_amount: number; thirteenth_salary_amount: number; employer_cpp_amount: number; fgts_amount: number; excluded_profit_distribution_amount: number; excluded_rent_amount: number; excluded_interns_amount: number; excluded_mei_amount: number; created_at: string; updated_at: string }
        Insert: { id?: string; period_id: string; company_id: string; employees_amount?: number; pro_labore_amount?: number; individual_contractors_amount?: number; thirteenth_salary_amount?: number; employer_cpp_amount?: number; fgts_amount?: number; excluded_profit_distribution_amount?: number; excluded_rent_amount?: number; excluded_interns_amount?: number; excluded_mei_amount?: number; created_at?: string; updated_at?: string }
        Update: { id?: string; period_id?: string; company_id?: string; employees_amount?: number; pro_labore_amount?: number; individual_contractors_amount?: number; thirteenth_salary_amount?: number; employer_cpp_amount?: number; fgts_amount?: number; excluded_profit_distribution_amount?: number; excluded_rent_amount?: number; excluded_interns_amount?: number; excluded_mei_amount?: number; created_at?: string; updated_at?: string }
      }
      simple_national_calculations: {
        Row: { id: string; period_id: string; company_id: string; rule_version_code: string; result_payload: Json; memory_payload: Json; created_at: string; updated_at: string }
        Insert: { id?: string; period_id: string; company_id: string; rule_version_code: string; result_payload?: Json; memory_payload?: Json; created_at?: string; updated_at?: string }
        Update: { id?: string; period_id?: string; company_id?: string; rule_version_code?: string; result_payload?: Json; memory_payload?: Json; created_at?: string; updated_at?: string }
      }
      nfs_stats: {
        Row: { id: string; company_id: string; period: string; qty_emitidas: number; qty_recebidas: number; valor_emitidas: number; valor_recebidas: number; service_codes: Json; service_codes_emitidas: Json; service_codes_recebidas: Json; created_at: string; updated_at: string }
        Insert: { id?: string; company_id: string; period: string; qty_emitidas?: number; qty_recebidas?: number; valor_emitidas?: number; valor_recebidas?: number; service_codes?: Json; service_codes_emitidas?: Json; service_codes_recebidas?: Json; created_at?: string; updated_at?: string }
        Update: { id?: string; company_id?: string; period?: string; qty_emitidas?: number; qty_recebidas?: number; valor_emitidas?: number; valor_recebidas?: number; service_codes?: Json; service_codes_emitidas?: Json; service_codes_recebidas?: Json; created_at?: string; updated_at?: string }
      }
    }
    Views: { [_ in never]: never }
    Functions: { [_ in never]: never }
    Enums: { [_ in never]: never }
    CompositeTypes: { [_ in never]: never }
  }
}

type DatabaseWithoutInternals = Omit<Database, "__InternalSupabase">
type DefaultSchema = DatabaseWithoutInternals[Extract<keyof Database, "public">]

export type Tables<
  DefaultSchemaTableNameOrOptions extends
    | keyof (DefaultSchema["Tables"] & DefaultSchema["Views"])
    | { schema: keyof DatabaseWithoutInternals },
  TableName extends DefaultSchemaTableNameOrOptions extends {
    schema: keyof DatabaseWithoutInternals
  }
    ? keyof (DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Tables"] &
        DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Views"])
    : never = never,
> = DefaultSchemaTableNameOrOptions extends {
  schema: keyof DatabaseWithoutInternals
}
  ? (DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Tables"] &
      DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Views"])[TableName] extends {
      Row: infer R
    }
    ? R
    : never
  : DefaultSchemaTableNameOrOptions extends keyof (DefaultSchema["Tables"] & DefaultSchema["Views"])
    ? (DefaultSchema["Tables"] &
        DefaultSchema["Views"])[DefaultSchemaTableNameOrOptions] extends {
        Row: infer R
      }
      ? R
      : never
    : never

export type TablesInsert<
  DefaultSchemaTableNameOrOptions extends
    | keyof DefaultSchema["Tables"]
    | { schema: keyof DatabaseWithoutInternals },
  TableName extends DefaultSchemaTableNameOrOptions extends {
    schema: keyof DatabaseWithoutInternals
  }
    ? keyof DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Tables"]
    : never = never,
> = DefaultSchemaTableNameOrOptions extends {
  schema: keyof DatabaseWithoutInternals
}
  ? DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Tables"][TableName] extends {
      Insert: infer I
    }
    ? I
    : never
  : DefaultSchemaTableNameOrOptions extends keyof DefaultSchema["Tables"]
    ? DefaultSchema["Tables"][DefaultSchemaTableNameOrOptions] extends {
        Insert: infer I
      }
      ? I
      : never
    : never

export type TablesUpdate<
  DefaultSchemaTableNameOrOptions extends
    | keyof DefaultSchema["Tables"]
    | { schema: keyof DatabaseWithoutInternals },
  TableName extends DefaultSchemaTableNameOrOptions extends {
    schema: keyof DatabaseWithoutInternals
  }
    ? keyof DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Tables"]
    : never = never,
> = DefaultSchemaTableNameOrOptions extends {
  schema: keyof DatabaseWithoutInternals
}
  ? DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Tables"][TableName] extends {
      Update: infer U
    }
    ? U
    : never
  : DefaultSchemaTableNameOrOptions extends keyof DefaultSchema["Tables"]
    ? DefaultSchema["Tables"][DefaultSchemaTableNameOrOptions] extends {
        Update: infer U
      }
      ? U
      : never
    : never

export type Enums<
  DefaultSchemaEnumNameOrOptions extends
    | keyof DefaultSchema["Enums"]
    | { schema: keyof DatabaseWithoutInternals },
  EnumName extends DefaultSchemaEnumNameOrOptions extends {
    schema: keyof DatabaseWithoutInternals
  }
    ? keyof DatabaseWithoutInternals[DefaultSchemaEnumNameOrOptions["schema"]]["Enums"]
    : never = never,
> = DefaultSchemaEnumNameOrOptions extends {
  schema: keyof DatabaseWithoutInternals
}
  ? DatabaseWithoutInternals[DefaultSchemaEnumNameOrOptions["schema"]]["Enums"][EnumName]
  : DefaultSchemaEnumNameOrOptions extends keyof DefaultSchema["Enums"]
    ? DefaultSchema["Enums"][DefaultSchemaEnumNameOrOptions]
    : never

export type CompositeTypes<
  PublicCompositeTypeNameOrOptions extends
    | keyof DefaultSchema["CompositeTypes"]
    | { schema: keyof DatabaseWithoutInternals },
  CompositeTypeName extends PublicCompositeTypeNameOrOptions extends {
    schema: keyof DatabaseWithoutInternals
  }
    ? keyof DatabaseWithoutInternals[PublicCompositeTypeNameOrOptions["schema"]]["CompositeTypes"]
    : never = never,
> = PublicCompositeTypeNameOrOptions extends {
  schema: keyof DatabaseWithoutInternals
}
  ? DatabaseWithoutInternals[PublicCompositeTypeNameOrOptions["schema"]]["CompositeTypes"][CompositeTypeName]
  : PublicCompositeTypeNameOrOptions extends keyof DefaultSchema["CompositeTypes"]
    ? DefaultSchema["CompositeTypes"][PublicCompositeTypeNameOrOptions]
    : never

export const Constants = {
  public: {
    Enums: {},
  },
} as const
