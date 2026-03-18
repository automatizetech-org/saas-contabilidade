-- Bucket para assets de branding (logo, favicon). Público para leitura das URLs.
INSERT INTO storage.buckets (id, name, public, file_size_limit, allowed_mime_types)
VALUES (
  'branding-assets',
  'branding-assets',
  true,
  2097152,
  ARRAY['image/png', 'image/svg+xml', 'image/jpeg', 'image/webp', 'image/x-icon']::text[]
)
ON CONFLICT (id) DO UPDATE SET
  public = EXCLUDED.public,
  file_size_limit = EXCLUDED.file_size_limit,
  allowed_mime_types = EXCLUDED.allowed_mime_types;

-- Políticas: qualquer um pode ler (bucket público); apenas authenticated pode fazer upload/delete
DROP POLICY IF EXISTS "branding_assets_public_read" ON storage.objects;
CREATE POLICY "branding_assets_public_read"
  ON storage.objects FOR SELECT
  TO public
  USING (bucket_id = 'branding-assets');

DROP POLICY IF EXISTS "branding_assets_authenticated_upload" ON storage.objects;
CREATE POLICY "branding_assets_authenticated_upload"
  ON storage.objects FOR INSERT
  TO authenticated
  WITH CHECK (bucket_id = 'branding-assets');

DROP POLICY IF EXISTS "branding_assets_authenticated_update" ON storage.objects;
CREATE POLICY "branding_assets_authenticated_update"
  ON storage.objects FOR UPDATE
  TO authenticated
  USING (bucket_id = 'branding-assets');

DROP POLICY IF EXISTS "branding_assets_authenticated_delete" ON storage.objects;
CREATE POLICY "branding_assets_authenticated_delete"
  ON storage.objects FOR DELETE
  TO authenticated
  USING (bucket_id = 'branding-assets');
