create or replace function public.refresh_office_document_index_certidao_company(p_office_id uuid, p_company_id uuid)
returns void
language plpgsql
security definer
set search_path = public
as $$
begin
  delete from public.office_document_index
  where source = 'certidoes'
    and office_id = p_office_id
    and company_id = p_company_id;

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
  with certs_raw as (
    select
      se.id,
      se.office_id,
      se.company_id,
      se.created_at,
      public.try_parse_jsonb(se.payload) as payload
    from public.sync_events se
    where se.office_id = p_office_id
      and se.company_id = p_company_id
      and se.tipo = 'certidao_resultado'
  ),
  latest as (
    select distinct on (company_id, coalesce(payload ->> 'tipo_certidao', ''))
      id,
      office_id,
      company_id,
      created_at,
      payload
    from certs_raw
    where coalesce(payload ->> 'tipo_certidao', '') <> ''
    order by company_id, coalesce(payload ->> 'tipo_certidao', ''), created_at desc
  )
  select
    'certidoes',
    l.id,
    l.office_id,
    l.company_id,
    c.name,
    c.document,
    'certidoes',
    'CERTIDÃO - ' ||
      case lower(coalesce(l.payload ->> 'tipo_certidao', ''))
        when 'federal' then 'Federal'
        when 'fgts' then 'FGTS'
        when 'estadual_go' then 'Estadual (GO)'
        else coalesce(l.payload ->> 'tipo_certidao', 'Outra')
      end,
    null::text,
    case
      when lower(coalesce(l.payload ->> 'status', '')) in ('regular', 'negativa', 'empregador não cadastrado', 'empregador nao cadastrado') then 'negativa'
      when lower(coalesce(l.payload ->> 'status', '')) = 'positiva' then 'positiva'
      else 'irregular'
    end,
    nullif(btrim(coalesce(l.payload ->> 'periodo', '')), ''),
    coalesce(nullif(l.payload ->> 'document_date', ''), nullif(l.payload ->> 'data_consulta', ''))::date,
    l.created_at,
    nullif(btrim(coalesce(l.payload ->> 'arquivo_pdf', '')), ''),
    null::text,
    null::text,
    lower(coalesce(l.payload ->> 'tipo_certidao', '')),
    case
      when lower(coalesce(l.payload ->> 'arquivo_pdf', '')) like '%.pdf' then 'pdf'
      when lower(coalesce(l.payload ->> 'arquivo_pdf', '')) like '%.xml' then 'xml'
      else null
    end,
    public.office_document_search_blob(
      c.name,
      c.document,
      'CERTIDÃO - ' ||
        case lower(coalesce(l.payload ->> 'tipo_certidao', ''))
          when 'federal' then 'Federal'
          when 'fgts' then 'FGTS'
          when 'estadual_go' then 'Estadual (GO)'
          else coalesce(l.payload ->> 'tipo_certidao', 'Outra')
        end,
      case
        when lower(coalesce(l.payload ->> 'status', '')) in ('regular', 'negativa', 'empregador não cadastrado', 'empregador nao cadastrado') then 'negativa'
        when lower(coalesce(l.payload ->> 'status', '')) = 'positiva' then 'positiva'
        else 'irregular'
      end,
      nullif(btrim(coalesce(l.payload ->> 'periodo', '')), ''),
      nullif(btrim(coalesce(l.payload ->> 'arquivo_pdf', '')), ''),
      null,
      null,
      null,
      lower(coalesce(l.payload ->> 'tipo_certidao', ''))
    ),
    now()
  from latest l
  join public.companies c
    on c.id = l.company_id
   and c.office_id = l.office_id;
end;
$$;
