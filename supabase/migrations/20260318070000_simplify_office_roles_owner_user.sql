update public.office_memberships
set role = 'viewer'
where role <> 'owner';

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
        and om.role = 'owner'
    )
$$;
