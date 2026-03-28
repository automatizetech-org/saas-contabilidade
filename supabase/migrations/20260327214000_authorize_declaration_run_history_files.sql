create or replace function public.get_authorized_file_paths(requested_paths text[] default null)
returns table (file_path text)
language sql
stable
security definer
set search_path = public
as $$
  with params as (
    select
      auth.uid() as user_id,
      public.current_office_id() as office_id,
      coalesce(
        array(
          select distinct replace(btrim(raw_path), '\', '/')
          from unnest(coalesce(requested_paths, '{}'::text[])) as raw_path
          where nullif(btrim(raw_path), '') is not null
        ),
        '{}'::text[]
      ) as requested_paths
  ),
  declaration_history_paths as (
    select distinct
      case
        when connector_base_path <> ''
          and lower(normalized_file_path) like lower(connector_base_path) || '/%'
          then substr(normalized_file_path, length(connector_base_path) + 2)
        else normalized_file_path
      end as file_path
    from public.declaration_run_history drh
    join params p
      on p.office_id = drh.office_id
    cross join lateral jsonb_array_elements(coalesce(drh.payload -> 'items', '[]'::jsonb)) as item(value)
    cross join lateral (
      select
        replace(
          nullif(
            btrim(
              coalesce(
                item.value -> 'artifact' ->> 'filePath',
                item.value -> 'meta' ->> 'file_path',
                item.value -> 'meta' ->> 'document_path',
                ''
              )
            ),
            ''
          ),
          '\',
          '/'
        ) as normalized_file_path,
        lower(
          replace(
            nullif(
              btrim(
                coalesce(
                  item.value -> 'meta' -> 'connector' ->> 'base_path',
                  item.value -> 'meta' -> 'connector' ->> 'basePath',
                  ''
                )
              ),
              ''
            ),
            '\',
            '/'
          )
        ) as connector_base_path
    ) as direct_file
    where direct_file.normalized_file_path <> ''

    union

    select distinct
      case
        when connector_base_path <> ''
          and lower(normalized_file_path) like lower(connector_base_path) || '/%'
          then substr(normalized_file_path, length(connector_base_path) + 2)
        else normalized_file_path
      end as file_path
    from public.declaration_run_history drh
    join params p
      on p.office_id = drh.office_id
    cross join lateral jsonb_array_elements(coalesce(drh.payload -> 'items', '[]'::jsonb)) as item(value)
    cross join lateral jsonb_array_elements(coalesce(item.value -> 'meta' -> 'files', '[]'::jsonb)) as file_entry(value)
    cross join lateral (
      select
        replace(
          nullif(
            btrim(
              coalesce(
                file_entry.value ->> 'path',
                file_entry.value ->> 'file_path',
                file_entry.value ->> 'relative_path',
                ''
              )
            ),
            ''
          ),
          '\',
          '/'
        ) as normalized_file_path,
        lower(
          replace(
            nullif(
              btrim(
                coalesce(
                  item.value -> 'meta' -> 'connector' ->> 'base_path',
                  item.value -> 'meta' -> 'connector' ->> 'basePath',
                  ''
                )
              ),
              ''
            ),
            '\',
            '/'
          )
        ) as connector_base_path
    ) as meta_file
    where meta_file.normalized_file_path <> ''
  ),
  allowed_paths as (
    select distinct fd.file_path
    from public.fiscal_documents fd
    join params p on p.office_id = fd.office_id
    where nullif(btrim(coalesce(fd.file_path, '')), '') = any (p.requested_paths)

    union

    select distinct g.file_path
    from public.dp_guias g
    join params p on p.office_id = g.office_id
    where nullif(btrim(coalesce(g.file_path, '')), '') = any (p.requested_paths)

    union

    select distinct m.guia_pdf_path as file_path
    from public.municipal_tax_debts m
    join params p on p.office_id = m.office_id
    where nullif(btrim(coalesce(m.guia_pdf_path, '')), '') = any (p.requested_paths)

    union

    select distinct nullif(btrim(coalesce((se.payload::jsonb) ->> 'arquivo_pdf', '')), '') as file_path
    from public.sync_events se
    join params p on p.office_id = se.office_id
    where se.tipo = 'certidao_resultado'
      and nullif(btrim(coalesce((se.payload::jsonb) ->> 'arquivo_pdf', '')), '') = any (p.requested_paths)

    union

    select distinct dhp.file_path
    from declaration_history_paths dhp
    join params p on true
    where nullif(btrim(coalesce(dhp.file_path, '')), '') = any (p.requested_paths)
  )
  select ap.file_path
  from allowed_paths ap
  join params p on true
  where p.user_id is not null
    and p.office_id is not null
    and ap.file_path is not null
$$;
