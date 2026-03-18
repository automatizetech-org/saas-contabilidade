-- Hardening/escala: quando houver summaries `office_*`, ler direto delas para evitar agregações pesadas em runtime.

create or replace function public.get_operations_overview_summary()
returns jsonb
language plpgsql
stable
security definer
set search_path = public
as $$
declare
  v_office_id uuid := public.current_office_id();
  v_payload jsonb;
begin
  -- Se já existir summary pré-computado, usar para evitar custo em runtime.
  select os.summary_payload into v_payload
  from public.office_operations_summary os
  where os.office_id = v_office_id;

  if v_payload is not null and v_payload <> '{}'::jsonb then
    return v_payload;
  end if;

  with completed as (
    select *
    from public.execution_requests e
    where e.office_id = v_office_id
      and e.status in ('completed', 'failed')
  ),
  summary as (
    select
      count(*) filter (where coalesce(completed_at, created_at) >= date_trunc('day', now()))::int as eventos_hoje,
      count(*) filter (
        where coalesce(completed_at, created_at) >= date_trunc('day', now()) - interval '1 day'
          and coalesce(completed_at, created_at) < date_trunc('day', now())
      )::int as eventos_ontem,
      count(*) filter (where status = 'completed')::int as success_count,
      count(*) filter (where status = 'failed')::int as fail_count,
      count(*)::int as total_count
    from completed
  ),
  robot_summary as (
    select count(*)::int as robots_count
    from public.robots
  )
  select jsonb_build_object(
    'eventosHoje', coalesce(summary.eventos_hoje, 0),
    'eventosOntem', coalesce(summary.eventos_ontem, 0),
    'falhas', coalesce(summary.fail_count, 0),
    'robots', coalesce(robot_summary.robots_count, 0),
    'taxaSucesso',
      case
        when coalesce(summary.total_count, 0) = 0 then 0
        else round((coalesce(summary.success_count, 0)::numeric / summary.total_count::numeric) * 1000) / 10
      end
  ) into v_payload
  from summary
  cross join robot_summary;

  return v_payload;
end;
$$;

revoke all on function public.get_operations_overview_summary() from public;
grant execute on function public.get_operations_overview_summary() to authenticated;

revoke all on function public.get_ir_overview_summary(text) from public;
grant execute on function public.get_ir_overview_summary(text) to authenticated;

create or replace function public.get_ir_overview_summary(responsavel_filter text default null)
returns jsonb
language plpgsql
stable
security definer
set search_path = public
as $$
declare
  v_office_id uuid := public.current_office_id();
  v_filter text := nullif(btrim(responsavel_filter), '');
  v_payload jsonb;
begin
  -- Resumo agregado por office (sem filtro por responsável).
  if v_filter is null then
    select irs.summary_payload into v_payload
    from public.office_ir_summary irs
    where irs.office_id = v_office_id;

    if v_payload is not null and v_payload <> '{}'::jsonb then
      return v_payload;
    end if;
  end if;

  -- Fallback (ou filtro não-nulo): calcula no runtime a agregação filtrada.
  return (
    with params as (
      select v_office_id as office_id, v_filter as responsavel_filter
    ),
    rows as (
      select *
      from public.ir_clients c
      join params p on p.office_id = c.office_id
      where p.responsavel_filter is null
        or coalesce(c.responsavel_ir, '') = p.responsavel_filter
    ),
    summary as (
      select
        count(*)::int as total,
        count(*) filter (where status_pagamento <> 'A PAGAR')::int as paid_count,
        count(*) filter (where status_pagamento = 'A PAGAR')::int as pending_count,
        count(*) filter (where status_declaracao = 'Concluido')::int as concluded_count,
        count(*) filter (where status_declaracao <> 'Concluido')::int as pending_execution_count,
        coalesce(sum(valor_servico), 0)::numeric as total_value,
        coalesce(sum(valor_servico) filter (where status_pagamento <> 'A PAGAR'), 0)::numeric as paid_value,
        coalesce(sum(valor_servico) filter (where status_pagamento = 'A PAGAR'), 0)::numeric as pending_value
      from rows
    )
    select jsonb_build_object(
      'cards',
      jsonb_build_object(
        'clientesIr', coalesce((select total from summary), 0),
        'recebidos', coalesce((select paid_count from summary), 0),
        'aPagar', coalesce((select pending_count from summary), 0),
        'concluidoPercent',
          case
            when coalesce((select total from summary), 0) = 0 then 0
            else round((coalesce((select concluded_count from summary), 0)::numeric / (select total from summary)::numeric) * 100)
          end,
        'concluidoTotal', coalesce((select concluded_count from summary), 0),
        'clientesTotal', coalesce((select total from summary), 0),
        'valorTotal', coalesce((select total_value from summary), 0)
      ),
      'progressData',
      jsonb_build_array(
        jsonb_build_object('name', 'Concluídos', 'value', coalesce((select concluded_count from summary), 0)),
        jsonb_build_object('name', 'Pendentes', 'value', coalesce((select pending_execution_count from summary), 0))
      ),
      'paymentValueData',
      jsonb_build_array(
        jsonb_build_object('name', 'Recebido', 'value', coalesce((select paid_value from summary), 0)),
        jsonb_build_object('name', 'A PAGAR', 'value', coalesce((select pending_value from summary), 0))
      ),
      'paidValuePercent',
        case
          when coalesce((select total_value from summary), 0) = 0 then 0
          else round((coalesce((select paid_value from summary), 0) / (select total_value from summary)) * 100)
        end
    )
  );
end;
$$;

