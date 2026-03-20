update public.robots
set
  status = 'inactive',
  last_heartbeat_at = null,
  updated_at = now()
where status is distinct from 'inactive'
   or last_heartbeat_at is not null;

do $$
begin
  if exists (
    select 1
    from information_schema.tables
    where table_schema = 'public'
      and table_name = 'office_robot_runtime'
  ) then
    update public.office_robot_runtime
    set
      status = 'inactive',
      last_heartbeat_at = null,
      current_execution_request_id = null,
      current_job_id = null,
      heartbeat_payload = '{}'::jsonb,
      updated_at = now()
    where status is distinct from 'inactive'
       or last_heartbeat_at is not null
       or current_execution_request_id is not null
       or current_job_id is not null;
  end if;
end
$$;
