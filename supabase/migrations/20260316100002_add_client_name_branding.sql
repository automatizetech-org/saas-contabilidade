-- Nome da marca (ex.: "Transparência") — substitui "Fleury" na interface. Se vazio, usa "Dashboard" / "Analytics" sem sufixo.
ALTER TABLE public.client_branding_settings
  ADD COLUMN IF NOT EXISTS client_name text;

COMMENT ON COLUMN public.client_branding_settings.client_name IS 'Nome da marca exibido na interface (ex.: Dashboard Transparência). Se null/vazio, exibe apenas Dashboard / Analytics.';
