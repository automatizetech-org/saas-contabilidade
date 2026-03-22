create or replace function public.office_document_index_merge_before_insert()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
begin
  update public.office_document_index
     set office_id = new.office_id,
         company_id = new.company_id,
         empresa = new.empresa,
         cnpj = new.cnpj,
         category_key = new.category_key,
         type = new.type,
         origem = new.origem,
         status = new.status,
         periodo = new.periodo,
         document_date = new.document_date,
         created_at = new.created_at,
         file_path = new.file_path,
         chave = new.chave,
         modelo = new.modelo,
         tipo_certidao = new.tipo_certidao,
         file_extension = new.file_extension,
         search_text_normalized = new.search_text_normalized,
         updated_at = coalesce(new.updated_at, now())
   where source = new.source
     and source_record_id = new.source_record_id;

  if found then
    return null;
  end if;

  return new;
end;
$$;

drop trigger if exists office_document_index_merge_before_insert_tg on public.office_document_index;

create trigger office_document_index_merge_before_insert_tg
before insert on public.office_document_index
for each row
execute function public.office_document_index_merge_before_insert();
