"""Regra de precificacao - margens"""
from psycopg import sql

SQL = sql.SQL("""

select
    base.id_base,
	base.idfilial,
	base.idfilialsaldo,
	base.idgrupopreco,
	base.icms,
	base.pis as pis,
	base.cofins as cofins,
	base.margem,
	base.adicional,
	base.frete,
	classificacao.classificacao,
 	preco_produto.idproduto, 
 	preco_produto.idgradex,
 	preco_produto.idgradey, 	
	marca.idmarca,
	origem.origem, 
	ncm.ncm,
	integracao.recuperacao_st,
	integracao.ler_api_frete,
	(
	case
		when ncm.ncm is not null then 10
		when preco_produto.idproduto is not null then 9
		when marca.idmarca is not null and classificacao.classificacao is not null then 8
		when marca.idmarca is not null and not classificacao.classificacao is null then 7
		when classificacao.classificacao is not null and marca.idmarca is null then 6
		else 1
	end
	* (case when origem.origem is not null then 2 else 1 end)
	) as prioridade,
    (
	case
		when ncm.ncm is not null then 'Ncm'
		when preco_produto.idproduto is not null then 'Idproduto'
		when marca.idmarca is not null and classificacao.classificacao is not null then 'Marca+Departamento'
		when marca.idmarca is not null and not classificacao.classificacao is null then 'Marca'
		when classificacao.classificacao is not null and marca.idmarca is null then 'Departamento'
        when origem.origem is not null then 'Origem'
		else 'Geral'
	end
	) as regra,
    base.agrupar_x_y,
    integracao.desoneracao_piscofins,
	integracao.customedio_campo,
    base.interestadual,
    count(1) over (partition by base.id_base) as n_id_base
from
	ecode.preco_base base
left join ecode.preco_classificacao classificacao 
   	on base.id_base = classificacao.id_base
   	and classificacao.situacao = 'Ativo'
left join ecode.preco_origem_produto origem 
   	on base.id_base = origem.id_base
   	and origem.situacao = 'Ativo'
left join ecode.preco_marca marca 
   	on base.id_base = marca.id_base
   	and marca.situacao = 'Ativo'
left join ecode.preco_ncm ncm 
   	on base.id_base = ncm.id_base
   	and ncm.situacao = 'Ativo'
left join ecode.preco_integracao integracao 
   	on base.id_base = integracao.id_base
   	and integracao.situacao = 'Ativo'
left join ecode.preco_prazomedio preco_prazomedio
    on preco_prazomedio.id_base = base.id_base 
    and preco_prazomedio.situacao = 'Ativo'
left join ecode.preco_produto preco_produto
	on preco_produto.id_base = base.id_base 
	and preco_produto.situacao = 'Ativo'
left join ecode.preco_pessoa preco_pessoa
	on preco_pessoa.id_base = base.id_base 
	and preco_pessoa.situacao = 'Ativo'
where 
  	base.situacao = 'Ativo'
    and base.idgrupopreco between 1279 and 1303
order by 
  base.idgrupopreco,
  base.idfilial,
  base.idfilialsaldo,
  prioridade desc,
  ncm.ncm nulls last,
  preco_produto.idproduto nulls last, 
  preco_produto.idgradex nulls last,
  preco_produto.idgradey nulls last,
  marca.idmarca nulls last,
  origem.origem nulls last,
  length(classificacao.classificacao) desc nulls last, 
  classificacao.classificacao asc nulls last,
  n_id_base 
""")
