update public.robots
set
  runtime_folder = 'simples_nacional_debitos',
  entrypoint_relpath = 'ecac_simples_debitos.py',
  updated_at = now()
where technical_id = 'ecac_simples_debitos';

update public.robots
set
  runtime_folder = 'simples_nacional_consulta_extratos_defis',
  entrypoint_relpath = 'ecac_simples_consulta_extratos_defis.py',
  updated_at = now()
where technical_id = 'ecac_simples_consulta_extratos_defis';
