-- Licenças dos robôs desktop (SEFAZ, NFS, Certidões, etc.)
-- O cliente chama apenas RPC verify_license com anon key (sem service_role).
-- Aplicar no projeto Supabase: SQL Editor ou `supabase db push`.

CREATE TABLE IF NOT EXISTS public.licenses (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  license_key text NOT NULL UNIQUE,
  client_name text,
  expires_at timestamptz,
  is_active boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS licenses_license_key_idx ON public.licenses (license_key);

ALTER TABLE public.licenses ENABLE ROW LEVEL SECURITY;

-- Sem políticas para anon/authenticated: leitura só via função SECURITY DEFINER.
-- (Quem administra insere linhas com service_role / dashboard SQL.)

CREATE OR REPLACE FUNCTION public.verify_license(p_key text)
RETURNS TABLE (
  expires_at timestamptz,
  client_name text
)
LANGUAGE sql
SECURITY DEFINER
SET search_path = public
STABLE
AS $$
  SELECT l.expires_at, l.client_name
  FROM public.licenses l
  WHERE l.license_key = trim(p_key)
    AND l.is_active = true
    AND (l.expires_at IS NULL OR l.expires_at >= now());
$$;

GRANT EXECUTE ON FUNCTION public.verify_license(text) TO anon, authenticated, service_role;

COMMENT ON TABLE public.licenses IS 'Chaves de licença dos robôs desktop; validação via verify_license().';
COMMENT ON FUNCTION public.verify_license(text) IS 'PostgREST RPC: valida p_key ativa e não expirada.';

-- Depois de aplicar, cadastre as chaves (SQL Editor com role que consiga INSERT):
--
-- insert into public.licenses (license_key, client_name, expires_at, is_active)
-- values (
--   'SUA_CHAVE_AQUI',
--   'Nome do cliente',
--   timestamptz '2027-12-31 23:59:59+00',  -- ou NULL para sem data de expiração
--   true
-- );
