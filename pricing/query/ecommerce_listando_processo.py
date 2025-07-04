"""SQL vendas listagem preco medio"""
from psycopg import sql

SQL = sql.SQL("""
with
  base_venda as (
  select 
  1 as idprocesso,
  upper(processo.descricao) as descricao,
  cidade.uf,
  coalesce(upper(substring(
  case
    when strpos(upper(processo_titulo.descricao),'LUIZA') > 0 then '(MAGAZINE LUIZA)' 
    when strpos(upper(processo_titulo.descricao),'MERC. PAGO') > 0 then '(MERCADOLIVRE)'
    when processo_titulo.idprocesso in (3953,6163,7734) then '(ECOMMERCE)'

    else processo_titulo.descricao 
  end 
  FROM '\\\\(([^)]+)\\\\)')),processo_titulo.descricao) as descricao_processo
from 
  rst.itembase itembase 
  left join glb.pessoa pessoa using (idcnpj_cpf)
  left join rst.enderecobase enderecobase using (idfilial, idpedidovenda)
  left join glb.processo processo on processo.idprocesso = itembase.idprocessomestre
  left join glb.cidade cidade using (idcidade)
  left join rst.titulopedidovenda titulopedidovenda 
    on titulopedidovenda.idtipotitulo = 1
    and titulopedidovenda.idfilial = itembase.idfilial
    and titulopedidovenda.idpedidovenda = itembase.idpedidovenda
  left join rst.titulo titulo 
    on titulo.idfilial = titulopedidovenda.idfilial
    and titulo.idtipotitulo = titulopedidovenda.idtipotitulo
    and titulo.idtitulo = titulopedidovenda.idtitulo
  left join glb.processo processo_titulo
    on processo_titulo.idprocesso = titulo.idprocesso
where 
  itembase.idfilial = 10083
  and pessoa.idtipopessoa = 1
  and itembase.datamovimento between CURRENT_DATE - 31 and CURRENT_DATE -1
  and itembase.idoperacaoproduto = 102010
  and itembase.idprocessomestre in (8072,8110,8286,8388,8398,8880,9880)
  and titulo.idprocesso not in (6390,6669,7581)
  ),
  sumarizando as (
  
   select 
    *,
    sum(pcto) over (
    partition by idprocesso, descricao_processo 
    order by pcto desc
    rows between unbounded preceding and current row
    ) as acm
  from (
  	  
		  select 
		    *,
		    round(venda_uf::numeric / venda_processo::numeric * 100,2) as pcto
		  from (
		  select 
		    distinct on (1,2,3)
		    idprocesso,
		    descricao_processo, 
		    uf, 
		    count(1) over (partition by idprocesso, descricao_processo, uf) as venda_uf,
		    count(1) over (partition by idprocesso, descricao_processo) as venda_processo
		  from 
		    base_venda
		    ) tmp
		  -- order by 
		   --  1,pcto desc
  
  ) tmp2
  
  )
  
  select descricao_processo, uf, pcto, acm from sumarizando
""")
