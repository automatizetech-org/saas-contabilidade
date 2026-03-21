create table if not exists public.ecac_mailbox_messages (
  id uuid primary key default gen_random_uuid(),
  office_id uuid not null default public.current_office_id() references public.offices(id) on delete cascade,
  company_id uuid not null references public.companies(id) on delete cascade,
  robot_technical_id text,
  external_message_id text not null,
  subject text not null,
  sender_name text,
  sender_document text,
  category text,
  status text not null default 'novo',
  received_at timestamptz not null,
  fetched_at timestamptz not null default now(),
  is_read boolean not null default false,
  read_at timestamptz,
  payload jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint ecac_mailbox_messages_status_check check (status in ('novo', 'lido', 'arquivado'))
);

create unique index if not exists ecac_mailbox_messages_office_company_external_uidx
  on public.ecac_mailbox_messages (office_id, company_id, external_message_id);

create index if not exists ecac_mailbox_messages_office_received_idx
  on public.ecac_mailbox_messages (office_id, received_at desc, created_at desc);

create index if not exists ecac_mailbox_messages_office_unread_idx
  on public.ecac_mailbox_messages (office_id, is_read, received_at desc)
  where is_read = false;

create index if not exists ecac_mailbox_messages_company_received_idx
  on public.ecac_mailbox_messages (company_id, received_at desc);

alter table public.ecac_mailbox_messages enable row level security;

drop policy if exists ecac_mailbox_messages_select on public.ecac_mailbox_messages;
create policy ecac_mailbox_messages_select
  on public.ecac_mailbox_messages
  for select
  to authenticated
  using (public.can_view_office(office_id));

drop policy if exists ecac_mailbox_messages_write on public.ecac_mailbox_messages;
create policy ecac_mailbox_messages_write
  on public.ecac_mailbox_messages
  for all
  to authenticated
  using (public.can_manage_office(office_id))
  with check (public.can_manage_office(office_id));

drop trigger if exists ecac_mailbox_messages_set_updated_at on public.ecac_mailbox_messages;
create trigger ecac_mailbox_messages_set_updated_at
  before update on public.ecac_mailbox_messages
  for each row execute procedure public.set_updated_at();
