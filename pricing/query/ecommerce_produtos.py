"""Regra de precificacao - margens"""
from psycopg import sql

SQL = sql.SQL("""
              
with 
  produtos as (
select 
  %(idfilial)s as idfilial,
  produtogradefilial.idproduto,
  produtogradefilial.idgradex,
  produtogradefilial.idgradey,
  produto.descricao,
  case 
  	when %(idfilial)s = 10083 
      then coalesce(nullif(coalesce(customedio.custo_calc_unit,0) - coalesce(customedio.vlr_icms_st_recup_calc,0),0),produtogradefilial.customedio,0)
      else coalesce(nullif(coalesce(customedio.custo_calc_unit,0),0),produtogradefilial.customedio,0)
  end + coalesce(customedio.vlr_icms_proprio_entrada_unit,0) as customedio,
  produto.iddepartamento,
  produto.idmarca,
  produto.idcodigonbm,
  produto.idsituacaoorigem,
  departamento.classificacao,
  departamento.iddepartamento,
  coalesce(customedio.vlr_icms_st_recup_calc,0) as vlr_icms_st_recup_calc,
  first_value(
case 
  	when %(idfilial)s = 10083 
      then coalesce(nullif(coalesce(customedio.custo_calc_unit,0) - coalesce(customedio.vlr_icms_st_recup_calc,0),0),produtogradefilial.customedio,0)
      else coalesce(nullif(coalesce(customedio.custo_calc_unit,0),0),produtogradefilial.customedio,0)
  end + coalesce(customedio.vlr_icms_proprio_entrada_unit,0)) 
    over (partition by produtogradefilial.idfilial,
    produtogradefilial.idproduto order by produtogradefilial.ultimaentrada desc nulls last,
    produtogradefilial.customedio desc) as customedio_agrupado,
    row_number() over (
    partition by produtogradefilial.idproduto, 
    produtogradefilial.idgradex,
    produtogradefilial.idgradey
    order by case when produtogradefilial.idfilial = %(idfilial)s then 0 else 1 end
    ) as posicao
    
from 
  rst.produtogradefilial produtogradefilial
  inner join glb.produtograde produtograde 
    using (idproduto, idgradex, idgradey)
  inner join glb.produto produto 
    using (idproduto)
  inner join glb.departamento departamento 
    using (iddepartamento)
  left join ecode.preco_customedio customedio 
    on produtogradefilial.idfilial = customedio.idfilial 
	and produtogradefilial.idproduto = customedio.idproduto
	and produtogradefilial.idgradex = customedio.idgradex
	and produtogradefilial.idgradey = customedio.idgradey
where  
   (case 
	when %(idfilial)s in (10001,10083) then 
	  produtogradefilial.idfilial in (10001,10083)
	else 
	  produtogradefilial.idfilial = %(idfilial)s
	end)
  and (
  		public.getsaldoproduto(produtogradefilial.idfilial, produtogradefilial.idproduto,produtogradefilial.idgradex,produtogradefilial.idgradey, 1, 1)  > 0 or 
    	public.getsaldoproduto(produtogradefilial.idfilial, produtogradefilial.idproduto,produtogradefilial.idgradex,produtogradefilial.idgradey, 1, 4) > 0
       )
  and (produto.iddepartamento between (format('1%%s',rpad(coalesce(nullif(%(classificacao)s,''),'0'),9,'0')))::integer and (format('1%%s',rpad(coalesce(nullif(%(classificacao)s,''),'9'),9,'9')))::integer)
  and (customedio.idproduto = coalesce(%(idproduto)s,customedio.idproduto) 
  and customedio.idgradex = coalesce(%(idgradex)s,customedio.idgradex)
  and customedio.idgradey = coalesce(%(idgradey)s,customedio.idgradey))
  and produto.idmarca = coalesce(%(idmarca)s,produto.idmarca)
  and produto.idsituacaoorigem = coalesce(%(origem)s,produto.idsituacaoorigem)
  and produto.idcodigonbm = coalesce(%(ncm)s,produto.idcodigonbm)
  and produto.iddepartamento not between 1027000000 and 1027999999
  )
 select * from produtos p where posicao = 1
""")
