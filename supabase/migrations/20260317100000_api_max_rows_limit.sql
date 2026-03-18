-- Aumenta o limite de linhas por requisição da API REST (PostgREST).
-- Padrão é 1000. Valor alto só para não precisar alterar de novo; o uso real fica bem abaixo.
-- No Supabase Cloud, o método principal é: Dashboard > Project Settings > API > Max rows.

ALTER ROLE authenticator SET pgrst.db_max_rows = 10000000;
NOTIFY pgrst, 'reload config';
