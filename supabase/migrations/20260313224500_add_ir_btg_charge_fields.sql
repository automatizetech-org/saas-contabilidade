alter table public.ir_clients
  add column if not exists payment_charge_type text,
  add column if not exists payment_charge_status text not null default 'none',
  add column if not exists payment_charge_id text,
  add column if not exists payment_charge_correlation_id text,
  add column if not exists payment_provider text,
  add column if not exists payment_link text,
  add column if not exists payment_pix_copy_paste text,
  add column if not exists payment_pix_qr_code text,
  add column if not exists payment_boleto_pdf_base64 text,
  add column if not exists payment_boleto_barcode text,
  add column if not exists payment_boleto_digitable_line text,
  add column if not exists payment_paid_at timestamptz,
  add column if not exists payment_payer_name text,
  add column if not exists payment_payer_tax_id text,
  add column if not exists payment_generated_at timestamptz,
  add column if not exists payment_last_webhook_at timestamptz,
  add column if not exists payment_metadata jsonb not null default '{}'::jsonb;

update public.ir_clients
set payment_charge_status = 'none'
where payment_charge_status is null;

alter table public.ir_clients
  drop constraint if exists ir_clients_payment_charge_type_check;

alter table public.ir_clients
  add constraint ir_clients_payment_charge_type_check
    check (payment_charge_type in ('PIX', 'BOLETO', 'BOLETO_HIBRIDO') or payment_charge_type is null);

alter table public.ir_clients
  drop constraint if exists ir_clients_payment_charge_status_check;

alter table public.ir_clients
  add constraint ir_clients_payment_charge_status_check
    check (payment_charge_status in ('none', 'pending', 'paid', 'failed', 'cancelled'));
