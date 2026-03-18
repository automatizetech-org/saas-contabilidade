-- CAE (Cadastro de Atividade Econômica) na Prefeitura de Goiânia — usado pelo robô de taxas para selecionar empresa no portal
ALTER TABLE companies ADD COLUMN IF NOT EXISTS cae text;
