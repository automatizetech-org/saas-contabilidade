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
export type PlatformRole = "super_admin" | "user"
export type OfficeRole = "owner" | "admin" | "operator" | "viewer"
export type OfficeStatus = "draft" | "active" | "inactive"
export type OfficeServerStatus = "pending" | "online" | "offline" | "error" | "disabled"
export type RobotJobStatus = "pending" | "claimed" | "running" | "completed" | "failed" | "cancelled" | "timed_out"

type TableDef<Row, Insert = Partial<Row>, Update = Partial<Row>> = {
  Row: Row
  Insert: Insert
  Update: Update
}

export type Database = {
  __InternalSupabase: { PostgrestVersion: "14.1" }
  public: {
    Tables: {
      profiles: TableDef<{
        id: string
        username: string
        role: PlatformRole
        created_at: string
        updated_at: string
      }>
      offices: TableDef<{
        id: string
        name: string
        slug: string
        status: OfficeStatus
        created_at: string
        updated_at: string
      }>
      office_memberships: TableDef<{
        id: string
        office_id: string
        user_id: string
        role: OfficeRole
        panel_access: Record<string, boolean>
        is_default: boolean
        created_at: string
        updated_at: string
      }>
      office_servers: TableDef<{
        id: string
        office_id: string
        public_base_url: string
        base_path: string
        status: OfficeServerStatus
        is_active: boolean
        connector_version: string | null
        min_supported_connector_version: string | null
        last_seen_at: string | null
        last_job_at: string | null
        host_fingerprint: string | null
        base_path_fingerprint: string | null
        server_secret_hash: string | null
        created_at: string
        updated_at: string
      }>
      office_branding: TableDef<{
        id: string
        office_id: string
        display_name: string | null
        primary_color: string | null
        secondary_color: string | null
        accent_color: string | null
        logo_path: string | null
        favicon_path: string | null
        use_custom_palette: boolean
        use_custom_logo: boolean
        use_custom_favicon: boolean
        created_at: string
        updated_at: string
      }>
      office_audit_logs: TableDef<{
        id: string
        office_id: string | null
        actor_user_id: string | null
        action: string
        entity_type: string
        entity_id: string | null
        payload: Json
        created_at: string
      }>
      admin_settings: TableDef<{
        id: string
        office_id: string
        key: string
        value: string
        updated_at: string
      }>
      accountants: TableDef<{
        id: string
        office_id: string
        name: string
        cpf: string
        active: boolean
        created_at: string
        updated_at: string
      }>
      companies: TableDef<{
        id: string
        office_id: string
        name: string
        document: string | null
        created_by: string | null
        created_at: string
        updated_at: string
        active: boolean
        auth_mode: string | null
        cert_blob_b64: string | null
        cert_password: string | null
        cert_valid_until: string | null
        contador_nome: string | null
        contador_cpf: string | null
        state_registration: string | null
        state_code: string | null
        city_name: string | null
        cae: string | null
        sefaz_go_logins: Json
      }>
      company_robot_config: TableDef<{
        id: string
        office_id: string
        company_id: string
        robot_technical_id: string
        enabled: boolean
        auth_mode: "password" | "certificate"
        nfs_password: string | null
        selected_login_cpf: string | null
        created_at: string
        updated_at: string
      }>
      robots: TableDef<{
        id: string
        technical_id: string
        display_name: string
        status: "active" | "inactive" | "processing"
        last_heartbeat_at: string | null
        segment_path: string | null
        created_at: string
        updated_at: string
        notes_mode: RobotNotesMode | null
        date_execution_mode: "competencia" | "interval" | null
        initial_period_start: string | null
        initial_period_end: string | null
        last_period_end: string | null
        is_fiscal_notes_robot: boolean
        fiscal_notes_kind: FiscalNotesKind | null
        global_logins: Json
      }>
      folder_structure_nodes: TableDef<{
        id: string
        office_id: string
        parent_id: string | null
        name: string
        slug: string | null
        date_rule: "year" | "year_month" | "year_month_day" | null
        position: number
        created_at: string
        updated_at: string
      }>
      schedule_rules: TableDef<{
        id: string
        office_id: string
        company_ids: string[]
        robot_technical_ids: string[]
        notes_mode: RobotNotesMode | null
        period_start: string | null
        period_end: string | null
        run_at_date: string | null
        run_at_time: string
        run_daily: boolean
        execution_mode: RobotExecutionMode | null
        status: "active" | "paused" | "completed"
        last_run_at: string | null
        created_by: string | null
        created_at: string
        updated_at: string
      }>
      execution_requests: TableDef<{
        id: string
        office_id: string
        company_ids: string[]
        robot_technical_ids: string[]
        status: "pending" | "running" | "completed" | "failed"
        robot_id: string | null
        claimed_at: string | null
        completed_at: string | null
        error_message: string | null
        period_start: string | null
        period_end: string | null
        notes_mode: RobotNotesMode | null
        schedule_rule_id: string | null
        execution_mode: RobotExecutionMode | null
        execution_group_id: string | null
        execution_order: number | null
        created_at: string
        created_by: string | null
      }>
      robot_display_config: TableDef<{
        office_id: string
        robot_technical_id: string
        company_ids: string[]
        period_start: string | null
        period_end: string | null
        notes_mode: RobotNotesMode | null
        updated_at: string
      }>
      documents: TableDef<{ id: string; office_id: string; company_id: string; tipo: string; periodo: string; status: string; origem: string; document_date: string | null; arquivos: string[]; created_at: string }>
      fiscal_documents: TableDef<{ id: string; office_id: string; company_id: string; type: string; chave: string | null; periodo: string; status: string; document_date: string | null; file_path: string | null; last_downloaded_at: string | null; created_at: string; updated_at: string }>
      fiscal_pendencias: TableDef<{ id: string; office_id: string; company_id: string; tipo: string; periodo: string; status: string; created_at: string }>
      dp_checklist: TableDef<{ id: string; office_id: string; company_id: string; tarefa: string; competencia: string; status: string; created_at: string }>
      dp_guias: TableDef<{ id: string; office_id: string; company_id: string; nome: string; tipo: string; data: string; file_path: string | null; created_at: string }>
      financial_records: TableDef<{ id: string; office_id: string; company_id: string; periodo: string; valor_cents: number; status: string; pendencias_count: number; created_at: string; updated_at: string }>
      automation_data: TableDef<{ id: string; office_id: string; company_id: string; automation_id: string; date: string; count_1: number | null; count_2: number | null; count_3: number | null; amount_1: number | null; metadata: Json; created_at: string }>
      ir_settings: TableDef<{ id: string; office_id: string; payment_due_date: string | null; created_at: string; updated_at: string }>
      ir_clients: TableDef<{
        id: string
        office_id: string
        nome: string
        cpf_cnpj: string
        responsavel_ir: string | null
        vencimento: string | null
        valor_servico: number
        status_pagamento: "PIX" | "DINHEIRO" | "TRANSFERENCIA POUPANCA" | "PERMUTA" | "A PAGAR"
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
      }>
      municipal_tax_debts: TableDef<{ id: string; office_id: string; company_id: string; ano: number | null; tributo: string; numero_documento: string | null; data_vencimento: string | null; valor: number; situacao: string | null; portal_inscricao: string | null; portal_cai: string | null; detalhes: Json; fetched_at: string; created_at: string; updated_at: string; guia_pdf_path: string | null }>
      municipal_tax_collection_runs: TableDef<{ id: string; office_id: string; robot_technical_id: string; company_id: string | null; company_name: string | null; status: "pending" | "running" | "completed" | "failed"; started_at: string | null; finished_at: string | null; debts_found: number; error_message: string | null; metadata: Json; created_at: string; updated_at: string }>
      nfs_stats: TableDef<{ id: string; office_id: string; company_id: string; period: string; qty_emitidas: number; qty_recebidas: number; valor_emitidas: number; valor_recebidas: number; service_codes: Json; service_codes_emitidas: Json; service_codes_recebidas: Json; created_at: string; updated_at: string }>
      sync_events: TableDef<{ id: string; office_id: string; company_id: string | null; tipo: string; payload: string | null; status: string; idempotency_key: string | null; retries: number; created_at: string }>
      tax_rule_versions: TableDef<{ id: string; regime: string; scope: string; version_code: string; effective_from: string; effective_to: string | null; title: string; source_reference: string; source_url: string | null; payload: Json; created_at: string; updated_at: string }>
      simple_national_periods: TableDef<{ id: string; office_id: string; company_id: string; apuration_period: string; company_start_date: string | null; current_period_revenue: number; municipal_iss_rate: number | null; subject_to_factor_r: boolean; base_annex: string; activity_label: string | null; created_at: string; updated_at: string }>
      simple_national_entries: TableDef<{ id: string; office_id: string; period_id: string; company_id: string; reference_month: string; entry_type: "revenue" | "payroll"; amount: number; created_at: string; updated_at: string }>
      simple_national_historical_revenue_allocations: TableDef<{ id: string; office_id: string; period_id: string; company_id: string; reference_month: string; annex_code: string; amount: number; created_at: string; updated_at: string }>
      simple_national_revenue_segments: TableDef<{ id: string; office_id: string; period_id: string; company_id: string; segment_code: string; market_type: string; description: string | null; amount: number; display_order: number; created_at: string; updated_at: string }>
      simple_national_payroll_compositions: TableDef<{ id: string; office_id: string; period_id: string; company_id: string; employees_amount: number; pro_labore_amount: number; individual_contractors_amount: number; thirteenth_salary_amount: number; employer_cpp_amount: number; fgts_amount: number; excluded_profit_distribution_amount: number; excluded_rent_amount: number; excluded_interns_amount: number; excluded_mei_amount: number; created_at: string; updated_at: string }>
      simple_national_calculations: TableDef<{ id: string; office_id: string; period_id: string; company_id: string; rule_version_code: string; result_payload: Json; memory_payload: Json; created_at: string; updated_at: string }>
      robot_schedules: TableDef<{ id: string; office_id: string; robot_technical_id: string; company_ids: string[]; run_at_time: string; run_daily: boolean; run_at_date: string | null; execution_mode: RobotExecutionMode | null; notes_mode: RobotNotesMode | null; status: string; created_by: string | null; created_at: string; updated_at: string }>
      robot_jobs: TableDef<{ id: string; office_id: string; robot_schedule_id: string | null; robot_technical_id: string; company_ids: string[]; status: RobotJobStatus; attempt_count: number; claimed_at: string | null; claimed_by_server_id: string | null; timeout_at: string | null; last_error: string | null; result_payload: Json; created_at: string; updated_at: string; completed_at: string | null }>
      robot_job_logs: TableDef<{ id: string; office_id: string; robot_job_id: string; level: string; message: string; payload: Json; created_at: string }>
    }
    Views: { [_ in never]: never }
    Functions: { [_ in never]: never }
    Enums: { [_ in never]: never }
    CompositeTypes: { [_ in never]: never }
  }
}

type DatabaseWithoutInternals = Omit<Database, "__InternalSupabase">
type DefaultSchema = DatabaseWithoutInternals["public"]

export type Tables<
  DefaultSchemaTableNameOrOptions extends keyof DefaultSchema["Tables"] | { schema: keyof DatabaseWithoutInternals },
  TableName extends DefaultSchemaTableNameOrOptions extends { schema: keyof DatabaseWithoutInternals }
    ? keyof DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Tables"]
    : never = never,
> = DefaultSchemaTableNameOrOptions extends { schema: keyof DatabaseWithoutInternals }
  ? DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Tables"][TableName]["Row"]
  : DefaultSchemaTableNameOrOptions extends keyof DefaultSchema["Tables"]
    ? DefaultSchema["Tables"][DefaultSchemaTableNameOrOptions]["Row"]
    : never

export type TablesInsert<
  DefaultSchemaTableNameOrOptions extends keyof DefaultSchema["Tables"] | { schema: keyof DatabaseWithoutInternals },
  TableName extends DefaultSchemaTableNameOrOptions extends { schema: keyof DatabaseWithoutInternals }
    ? keyof DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Tables"]
    : never = never,
> = DefaultSchemaTableNameOrOptions extends { schema: keyof DatabaseWithoutInternals }
  ? DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Tables"][TableName]["Insert"]
  : DefaultSchemaTableNameOrOptions extends keyof DefaultSchema["Tables"]
    ? DefaultSchema["Tables"][DefaultSchemaTableNameOrOptions]["Insert"]
    : never

export type TablesUpdate<
  DefaultSchemaTableNameOrOptions extends keyof DefaultSchema["Tables"] | { schema: keyof DatabaseWithoutInternals },
  TableName extends DefaultSchemaTableNameOrOptions extends { schema: keyof DatabaseWithoutInternals }
    ? keyof DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Tables"]
    : never = never,
> = DefaultSchemaTableNameOrOptions extends { schema: keyof DatabaseWithoutInternals }
  ? DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Tables"][TableName]["Update"]
  : DefaultSchemaTableNameOrOptions extends keyof DefaultSchema["Tables"]
    ? DefaultSchema["Tables"][DefaultSchemaTableNameOrOptions]["Update"]
    : never

export const Constants = { public: { Enums: {} } } as const
