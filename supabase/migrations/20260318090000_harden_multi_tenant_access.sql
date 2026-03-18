create or replace function public.guard_profile_role_update()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
begin
  if auth.uid() is null then
    return new;
  end if;

  if auth.uid() = new.id
     and not public.is_platform_super_admin()
     and new.role is distinct from old.role then
    raise exception 'Apenas super_admin pode alterar o papel de plataforma.';
  end if;

  return new;
end;
$$;

drop trigger if exists profiles_guard_role_update on public.profiles;
create trigger profiles_guard_role_update
  before update on public.profiles
  for each row execute procedure public.guard_profile_role_update();
