create or replace function public.get_visible_robots()
returns table (
  id uuid,
  technical_id text,
  display_name text,
  status text,
  last_heartbeat_at timestamptz,
  segment_path text,
  created_at timestamptz,
  updated_at timestamptz,
  notes_mode text,
  date_execution_mode text,
  initial_period_start date,
  initial_period_end date,
  last_period_end date,
  is_fiscal_notes_robot boolean,
  fiscal_notes_kind text,
  global_logins jsonb
)
language sql
stable
security definer
set search_path = public
as $$
  with viewer as (
    select auth.uid() as user_id, public.is_platform_super_admin() as is_super_admin
  )
  select
    r.id,
    r.technical_id,
    r.display_name,
    r.status,
    r.last_heartbeat_at,
    r.segment_path,
    r.created_at,
    r.updated_at,
    r.notes_mode,
    r.date_execution_mode,
    r.initial_period_start,
    r.initial_period_end,
    r.last_period_end,
    r.is_fiscal_notes_robot,
    r.fiscal_notes_kind,
    case
      when v.is_super_admin then r.global_logins
      else coalesce((
        select jsonb_agg(
          jsonb_build_object(
            'cpf', public.only_digits(coalesce(item ->> 'cpf', item ->> 'login', item ->> 'username', '')),
            'password', case
              when nullif(btrim(coalesce(item ->> 'password', item ->> 'senha', '')), '') is not null then '__configured__'
              else null
            end,
            'is_default', coalesce((item ->> 'is_default')::boolean, false)
          )
        )
        from jsonb_array_elements(coalesce(r.global_logins, '[]'::jsonb)) as item
        where length(public.only_digits(coalesce(item ->> 'cpf', item ->> 'login', item ->> 'username', ''))) = 11
      ), '[]'::jsonb)
    end as global_logins
  from public.robots r
  cross join viewer v
  where v.user_id is not null
  order by r.display_name asc
$$;

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

    select distinct nullif(btrim(coalesce(se.payload ->> 'arquivo_pdf', '')), '') as file_path
    from public.sync_events se
    join params p on p.office_id = se.office_id
    where se.tipo = 'certidao_resultado'
      and nullif(btrim(coalesce(se.payload ->> 'arquivo_pdf', '')), '') = any (p.requested_paths)
  )
  select ap.file_path
  from allowed_paths ap
  join params p on true
  where p.user_id is not null
    and p.office_id is not null
    and ap.file_path is not null
$$;

drop policy if exists robots_select on public.robots;
create policy robots_select on public.robots
  for select to authenticated
  using (public.is_platform_super_admin());

drop policy if exists folder_structure_templates_select on public.folder_structure_templates;
create policy folder_structure_templates_select on public.folder_structure_templates
  for select to authenticated
  using (public.is_platform_super_admin());
