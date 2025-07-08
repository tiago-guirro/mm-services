"""Imposto Atacado"""
from psycopg import sql
SQL = sql.SQL("""
WITH 
filiais as (
	select 
	  distinct on (1,3,4)
	  case when multi_grupo.idfilial_faturamento in (10001,10083) then 10050 else multi_grupo.idfilial_faturamento end as idfilial,
	  cidade.uf as uf_origem,
	  multi_grupo.uf[1] as uf_destino,
	  multi_grupo.idgrupopreco
	from 
	  ecode.multi_grupo multi_grupo
	  inner join glb.filial filial on multi_grupo.idfilial_faturamento = filial.idfilial
	  inner join glb.endereco endereco using (idcnpj_cpf)
	  inner join glb.cidade cidade using (idcidade)
	where 
	  multi_grupo.situacao = 'Ativo'
) ,
agrupamento as (
	select
	  %(idproduto)s as idproduto,
	  ufs.idfilial,
	  ufs.idgrupopreco,
	  ufs.uf_origem,
	  ufs.uf_destino,
	  max(case when rf.idtipoimposto = 1 then rf.aliquota else 0 end) AS icms_origem,
	  max(case when rf.idtipoimposto = 3 then rf.aliquota else 0 end) as pis,
	  max(case when rf.idtipoimposto = 4 then rf.aliquota else 0 end) as cofins,
	  max(case when rf.idtipoimposto = 1 then rf.percentualbase else 0 end) AS percentualbase,
	  max(case when rf.idtipoimposto = 1 then rf.percentualdiferido else 0 end) AS percentualdiferido
	FROM filiais ufs
	CROSS JOIN LATERAL (
	  SELECT * FROM dsv.ret_regrafiscal_ecode2(
	    CASE WHEN ufs.uf_destino = ufs.uf_origem THEN 5102 ELSE 6102 END,
	    1,
	    CURRENT_DATE,
	    ufs.idfilial,
	    %(idproduto)s,
	    ufs.uf_origem,
	    ufs.uf_destino,
	    ''
	  ) x
	  WHERE 
	    x.idtipoimposto not in (11)
	) rf
	group by 1,2,3,4,5

)


select 
    distinct on (1,2,3,4)
	idproduto,
	idfilial,
    idgrupopreco,
	case 
		when uf_origem = 'PB' then
			case when uf_origem = uf_destino then 4.2 else 1.2 end
		else icms_origem
	end icms_origem,
	pis,
 cofins,
	percentualbase,
    percentualdiferido
from 
	agrupamento
order by 3

""")
