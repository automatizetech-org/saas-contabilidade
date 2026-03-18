-- Caminho relativo ao BASE_PATH do dashboard para o PDF da guia DUAM (ex.: EMPRESAS/Empresa X/PARALEGAL/TAXAS-IMPOSTOS/guia_2026_1759_2.pdf)
alter table public.municipal_tax_debts
  add column if not exists guia_pdf_path text;

comment on column public.municipal_tax_debts.guia_pdf_path is 'Path relativo ao BASE_PATH para download da guia em PDF (preenchido pelo robô Goiânia).';
