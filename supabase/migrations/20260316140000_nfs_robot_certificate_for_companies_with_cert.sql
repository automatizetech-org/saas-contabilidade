-- Marca auth_mode = 'certificate' no robô NFS Padrão para empresas que têm certificado ativo.
-- Certificado ativo: cert_blob_b64 preenchido e (cert_valid_until nulo ou >= hoje).

update public.company_robot_config
set
  auth_mode = 'certificate',
  updated_at = now()
where
  robot_technical_id = 'nfs_padrao'
  and company_id in (
    select id
    from public.companies
    where
      cert_blob_b64 is not null
      and trim(cert_blob_b64) <> ''
      and (cert_valid_until is null or cert_valid_until >= current_date)
  );
