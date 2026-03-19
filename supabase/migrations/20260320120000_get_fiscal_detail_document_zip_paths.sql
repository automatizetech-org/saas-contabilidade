-- RPC única para listar file_path + empresa para ZIP em /fiscal/nfs e /fiscal/nfe-nfc.
-- Mesmos filtros e segurança de get_fiscal_detail_documents_cursor, dedupe por file_path, limite 50000.
-- Evita N chamadas de paginação no front e deixa o ZIP tão rápido quanto em /documentos.

create or replace function public.get_fiscal_detail_document_zip_paths(
  detail_kind text,
  company_ids uuid[] default null,
  search_text text default null,
  date_from date default null,
  date_to date default null,
  file_kind text default null,
  origem_filter text default null,
  modelo_filter text default null,
  limit_count integer default 50000
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_office_id uuid := public.current_office_id();
  v_kind text := upper(coalesce(detail_kind, 'NFS'));
  v_has_index boolean;
  v_payload jsonb;
begin
  -- Apenas NFS e NFE_NFC têm ZIP nesta tela; certidões usam outro fluxo.
  if v_kind not in ('NFS', 'NFE_NFC') then
    return '[]'::jsonb;
  end if;

  select exists(select 1 from public.office_document_index where office_id = v_office_id limit 1)
  into v_has_index;

  if not v_has_index then
    perform public.refresh_office_document_index(v_office_id);
  end if;

  with params as (
    select
      coalesce(company_ids, '{}'::uuid[]) as company_ids,
      coalesce(array_length(company_ids, 1), 0) as company_count,
      public.normalize_search_text(search_text) as search_text,
      public.only_digits(search_text) as digits_search,
      least(date_from, date_to) as date_from,
      greatest(date_from, date_to) as date_to,
      lower(coalesce(nullif(btrim(file_kind), ''), 'all')) as file_kind,
      lower(coalesce(nullif(btrim(origem_filter), ''), 'all')) as origem_filter,
      lower(coalesce(nullif(btrim(modelo_filter), ''), 'all')) as modelo_filter,
      least(greatest(coalesce(limit_count, 50000), 1), 50000) as limit_count
  ),
  filtered as (
    select
      i.file_path,
      i.empresa,
      coalesce(i.document_date, i.created_at::date) as sort_date,
      i.created_at,
      i.source_record_id
    from public.office_document_index i
    join params p on true
    where i.office_id = v_office_id
      and i.file_path is not null
      and btrim(i.file_path) <> ''
      and (
        (v_kind = 'NFS' and i.source = 'fiscal' and i.type = 'NFS')
        or (v_kind = 'NFE_NFC' and i.source = 'fiscal' and i.type in ('NFE', 'NFC'))
      )
      and (p.company_count = 0 or i.company_id = any (p.company_ids))
      and (p.date_from is null or coalesce(i.document_date, i.created_at::date) >= p.date_from)
      and (p.date_to is null or coalesce(i.document_date, i.created_at::date) <= p.date_to)
      and (
        p.search_text = ''
        or i.search_text_normalized like '%' || p.search_text || '%'
        or (p.digits_search <> '' and public.only_digits(coalesce(i.cnpj, '')) like '%' || p.digits_search || '%')
        or coalesce(i.chave, '') like '%' || btrim(coalesce(search_text, '')) || '%'
      )
      and (
        p.file_kind = 'all'
        or (p.file_kind = 'xml' and i.file_extension = 'xml')
        or (p.file_kind = 'pdf' and i.file_extension = 'pdf')
      )
      and (
        p.origem_filter = 'all'
        or v_kind <> 'NFS'
        or coalesce(i.origem, '') = p.origem_filter
      )
      and (
        p.modelo_filter = 'all'
        or v_kind <> 'NFE_NFC'
        or coalesce(i.modelo, '') = p.modelo_filter
      )
  ),
  deduped as (
    select distinct on (file_path)
      file_path,
      empresa
    from filtered
    order by file_path, sort_date desc, created_at desc, source_record_id desc
    limit (select limit_count from params)
  )
  select coalesce(
    (select jsonb_agg(jsonb_build_object('file_path', file_path, 'empresa', empresa)) from deduped),
    '[]'::jsonb
  )
  into v_payload;

  return coalesce(v_payload, '[]'::jsonb);
end;
$$;

revoke all on function public.get_fiscal_detail_document_zip_paths(text, uuid[], text, date, date, text, text, text, integer) from public;
grant execute on function public.get_fiscal_detail_document_zip_paths(text, uuid[], text, date, date, text, text, text, integer) to authenticated;
