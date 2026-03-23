-- Notas NFE/NFC inseridas pelo robô Sefaz XML sem office_id não entram nas RPCs do SaaS
-- (join em fd.office_id = current_office_id) nem em office_document_index
-- (join c.office_id = fd.office_id). Preenche a partir de companies.

update public.fiscal_documents fd
set office_id = c.office_id
from public.companies c
where fd.company_id = c.id
  and c.office_id is not null
  and fd.office_id is null
  and fd.type in ('NFE', 'NFC');

-- O trigger fiscal_documents_projection_refresh (AFTER UPDATE) chama upsert_office_document_index_fiscal
-- para cada linha alterada, recolocando NFE/NFC no office_document_index.
