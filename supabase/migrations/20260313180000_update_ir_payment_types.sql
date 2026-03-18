alter table public.ir_clients
  alter column status_pagamento drop default;

update public.ir_clients
set status_pagamento = case
  when status_pagamento = 'Pendente' then 'A PAGAR'
  when status_pagamento = 'Pago' then 'PIX'
  when status_pagamento = 'Dinheiro' then 'DINHEIRO'
  when status_pagamento = 'Transferência Poupança' then 'TRANSFERÊNCIA POUPANÇA'
  when status_pagamento = 'Permuta' then 'PERMUTA'
  when status_pagamento = 'A Pagar' then 'A PAGAR'
  else status_pagamento
end;

alter table public.ir_clients
  drop constraint if exists ir_clients_status_pagamento_check;

alter table public.ir_clients
  add constraint ir_clients_status_pagamento_check
  check (
    status_pagamento in (
      'PIX',
      'DINHEIRO',
      'TRANSFERÊNCIA POUPANÇA',
      'PERMUTA',
      'A PAGAR'
    )
  );

alter table public.ir_clients
  alter column status_pagamento set default 'A PAGAR';
