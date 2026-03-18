-- Garantir que a tabela accountants existe (pode já existir em outros ambientes)
CREATE TABLE IF NOT EXISTS public.accountants (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  name text NOT NULL,
  cpf text NOT NULL,
  active boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

-- Índice para evitar duplicata por CPF e acelerar busca
CREATE UNIQUE INDEX IF NOT EXISTS accountants_cpf_key ON public.accountants (cpf);

-- RLS: permitir que usuários autenticados leiam e gerenciem contadores (sem isso o app não vê os registros)
ALTER TABLE public.accountants ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "accountants_authenticated_all" ON public.accountants;
CREATE POLICY "accountants_authenticated_all"
  ON public.accountants FOR ALL TO authenticated
  USING (true) WITH CHECK (true);

-- Anon também pode ler (caso algum fluxo use anon key para listar contadores)
DROP POLICY IF EXISTS "accountants_anon_select" ON public.accountants;
CREATE POLICY "accountants_anon_select"
  ON public.accountants FOR SELECT TO anon USING (true);

-- Inserir Elianderson e Eder como contadores no banco (não mais lista fixa)
-- para que possam ser editados, inativados e excluídos como qualquer outro.
INSERT INTO public.accountants (name, cpf, active, created_at, updated_at)
SELECT 'ELIANDERSON GOMES FLEURY', '71361170115', true, now(), now()
WHERE NOT EXISTS (SELECT 1 FROM public.accountants WHERE cpf = '71361170115');

INSERT INTO public.accountants (name, cpf, active, created_at, updated_at)
SELECT 'EDER GOMES FLEURY', '86873598100', true, now(), now()
WHERE NOT EXISTS (SELECT 1 FROM public.accountants WHERE cpf = '86873598100');
