create or replace function public.current_platform_role()
returns public.platform_role
language sql
stable
security definer
set search_path = public
as $$
  select role from public.profiles where id = auth.uid()
$$;

create or replace function public.is_platform_super_admin()
returns boolean
language sql
stable
security definer
set search_path = public
as $$
  select coalesce(public.current_platform_role() = 'super_admin', false)
$$;

create or replace function public.current_office_id()
returns uuid
language sql
stable
security definer
set search_path = public
as $$
  select office_id
  from public.office_memberships
  where user_id = auth.uid()
  order by is_default desc, created_at asc
  limit 1
$$;

create or replace function public.can_view_office(target_office_id uuid)
returns boolean
language sql
stable
security definer
set search_path = public
as $$
  select public.is_platform_super_admin()
    or exists (
      select 1
      from public.office_memberships om
      where om.user_id = auth.uid()
        and om.office_id = target_office_id
    )
$$;

create or replace function public.can_manage_office(target_office_id uuid)
returns boolean
language sql
stable
security definer
set search_path = public
as $$
  select public.is_platform_super_admin()
    or exists (
      select 1
      from public.office_memberships om
      where om.user_id = auth.uid()
        and om.office_id = target_office_id
        and om.role in ('owner', 'admin')
    )
$$;
