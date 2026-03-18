-- Ativa a opção "Não capturar débitos de ISS" no robô Goiânia.
-- Com isso, o robô (código atualizado) deixará de enviar débitos de ISS ao Supabase e de marcar para baixar guia.
update public.admin_settings
set value = 'true', updated_at = now()
where key = 'robot_goiania_skip_iss';

-- Se a chave não existir (migration anterior não rodou), insere.
insert into public.admin_settings (key, value, updated_at)
values ('robot_goiania_skip_iss', 'true', now())
on conflict (key) do update set value = 'true', updated_at = now();
