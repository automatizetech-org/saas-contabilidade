-- Índice fiscal: reconhecer pastas do robô Sefaz Xml (Notas Fiscais de Entrada/Saída, 55/65)
-- além de Recebidas/Emitidas legadas — melhora origem/modelo para filtros e busca.

create or replace function public.upsert_office_document_index_fiscal(p_document_id uuid)
returns void
language plpgsql
security definer
set search_path = public
as $$
begin
  delete from public.office_document_index
  where source = 'fiscal'
    and source_record_id = p_document_id;

  insert into public.office_document_index (
    source,
    source_record_id,
    office_id,
    company_id,
    empresa,
    cnpj,
    category_key,
    type,
    origem,
    status,
    periodo,
    document_date,
    created_at,
    file_path,
    chave,
    modelo,
    tipo_certidao,
    file_extension,
    search_text_normalized,
    updated_at
  )
  select
    'fiscal',
    fd.id,
    fd.office_id,
    fd.company_id,
    c.name,
    c.document,
    case
      when fd.type = 'NFS' then 'nfs'
      when fd.type in ('NFE', 'NFC') then 'nfe_nfc'
      else 'fiscal_outros'
    end,
    fd.type,
    case
      when lower(replace(coalesce(fd.file_path, ''), '\', '/')) like '%/recebidas/%' then 'recebidas'
      when lower(replace(coalesce(fd.file_path, ''), '\', '/')) like '%/emitidas/%' then 'emitidas'
      when lower(replace(coalesce(fd.file_path, ''), '\', '/')) like '%notas fiscais de entrada%' then 'recebidas'
      when lower(replace(coalesce(fd.file_path, ''), '\', '/')) like '%notas fiscais de saida%'
        or lower(replace(coalesce(fd.file_path, ''), '\', '/')) like '%notas fiscais de saída%' then 'emitidas'
      else null
    end,
    fd.status::text,
    fd.periodo,
    coalesce(fd.document_date, fd.created_at::date),
    fd.created_at,
    fd.file_path,
    fd.chave,
    case
      when replace(coalesce(fd.file_path, ''), '\', '/') like '%/55/%' then '55'
      when replace(coalesce(fd.file_path, ''), '\', '/') like '%/65/%' then '65'
      when fd.type = 'NFE' then '55'
      when fd.type = 'NFC' then '65'
      else null
    end,
    null::text,
    case
      when lower(coalesce(fd.file_path, '')) like '%.pdf' then 'pdf'
      when lower(coalesce(fd.file_path, '')) like '%.xml' then 'xml'
      else null
    end,
    public.office_document_search_blob(
      c.name,
      c.document,
      fd.type,
      fd.status::text,
      fd.periodo,
      fd.file_path,
      fd.chave,
      case
        when lower(replace(coalesce(fd.file_path, ''), '\', '/')) like '%/recebidas/%' then 'recebidas'
        when lower(replace(coalesce(fd.file_path, ''), '\', '/')) like '%/emitidas/%' then 'emitidas'
        when lower(replace(coalesce(fd.file_path, ''), '\', '/')) like '%notas fiscais de entrada%' then 'recebidas'
        when lower(replace(coalesce(fd.file_path, ''), '\', '/')) like '%notas fiscais de saida%'
          or lower(replace(coalesce(fd.file_path, ''), '\', '/')) like '%notas fiscais de saída%' then 'emitidas'
        else null
      end,
      case
        when replace(coalesce(fd.file_path, ''), '\', '/') like '%/55/%' then '55'
        when replace(coalesce(fd.file_path, ''), '\', '/') like '%/65/%' then '65'
        when fd.type = 'NFE' then '55'
        when fd.type = 'NFC' then '65'
        else null
      end,
      null
    ),
    now()
  from public.fiscal_documents fd
  join public.companies c
    on c.id = fd.company_id
   and c.office_id = fd.office_id
  where fd.id = p_document_id
    and coalesce(btrim(fd.file_path), '') <> '';
end;
$$;

-- Após aplicar a migration + deploy do server-api: rode sincronização fiscal no conector
-- (ou `select public.refresh_office_document_index('<office_id>');`) para reprojetar índices antigos.
