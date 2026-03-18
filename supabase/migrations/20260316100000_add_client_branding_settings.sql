-- Tabela de configuração de branding por cliente (client_id = 'default' para instância única)
CREATE TABLE IF NOT EXISTS public.client_branding_settings (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  client_id text NOT NULL DEFAULT 'default',
  primary_color text,
  secondary_color text,
  tertiary_color text,
  logo_url text,
  favicon_url text,
  use_custom_palette boolean NOT NULL DEFAULT false,
  use_custom_logo boolean NOT NULL DEFAULT false,
  use_custom_favicon boolean NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (client_id)
);

COMMENT ON TABLE public.client_branding_settings IS 'Configurações de identidade visual (cores, logo, favicon) por cliente. client_id=default para instância única.';

-- Trigger updated_at
CREATE OR REPLACE FUNCTION public.set_client_branding_updated_at()
RETURNS trigger AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS client_branding_settings_updated_at ON public.client_branding_settings;
CREATE TRIGGER client_branding_settings_updated_at
  BEFORE UPDATE ON public.client_branding_settings
  FOR EACH ROW EXECUTE PROCEDURE public.set_client_branding_updated_at();

-- RLS: leitura permitida para anon e authenticated (para login e dashboard); escrita apenas authenticated (admin restringe no app)
ALTER TABLE public.client_branding_settings ENABLE ROW LEVEL SECURITY;

CREATE POLICY "client_branding_select"
  ON public.client_branding_settings FOR SELECT
  TO anon, authenticated
  USING (true);

CREATE POLICY "client_branding_insert"
  ON public.client_branding_settings FOR INSERT
  TO authenticated
  WITH CHECK (true);

CREATE POLICY "client_branding_update"
  ON public.client_branding_settings FOR UPDATE
  TO authenticated
  USING (true)
  WITH CHECK (true);

CREATE POLICY "client_branding_delete"
  ON public.client_branding_settings FOR DELETE
  TO authenticated
  USING (true);

-- Inserir linha default para client_id = 'default' (opcional; o app pode upsert)
INSERT INTO public.client_branding_settings (client_id)
VALUES ('default')
ON CONFLICT (client_id) DO NOTHING;
