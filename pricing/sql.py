"""Dados dos sql do projeto."""

SQL_INIT_TEST = """

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
  greatest(produtogradefilial.ultimaentrada, produtogradefilial.ultimasaida, produtogradefilial.ultimaprevisaoareceber) >= CURRENT_DATE - interval '1 year'
  and (case 
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

  )
  
 select * from produtos p where posicao = 1
"""

SQL_LOAD_REGRA = """

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
    base.interestadual
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
order by 
  base.idfilial,
  base.idfilialsaldo,
  base.idgrupopreco,
  prioridade desc,
  ncm.ncm nulls last,
  length(classificacao.classificacao) desc, 
  classificacao.classificacao asc  nulls last
"""

ARVORE_DEPARTAMENTO = """
with recursive arvore_departamento (iddepartamento,idsubordinado,classificacao,descricao) as
(
	select 
		d1.iddepartamento, 
		d1.idsubordinado, 
		d1.classificacao,
		d1.descricao 
	from 
		glb.departamento d1 
	where 
		d1.iddepartamento = %s
	union 
	select 
		d1.iddepartamento, 
		d1.idsubordinado, 
		d1.classificacao,
		d1.descricao 
	from 
		glb.departamento d1 
		inner join ecode.arvore_departamento af on d1.iddepartamento = af.idsubordinado
)
select iddepartamento,classificacao,initcap(descricao) as descricao from glb.departamento order by 1

"""

ARVORE_IDPRODUTOS = """
select distinct iddepartamento from glb.produto 
"""

PRODUTOS_TSVECTOR = """
with 
  pesquisa as (
  	select 
	  produto.idproduto,
	  to_tsvector('pg_catalog.portuguese',
		  produto.idproduto::Varchar ||' '||
		  coalesce(trim(produto.descricao),'') ||' '||
		  coalesce(trim(produto.modelo),'') ||' '||
		  coalesce(marca."descricao",'')
	  ) as pesquisa
	from 
	  glb.produto produto
	  inner join glb.marca marca using (idmarca)
	  inner join glb.departamento departamento using (iddepartamento)
  ),
  atualizacao as (
	  insert into ecode.produto_pesquisa (idproduto, pesquisa)
	  select * from pesquisa
	  on conflict (idproduto) do update set
	  pesquisa = excluded.pesquisa
	  returning 1
  )
  select count(1) from atualizacao
"""

INSERT_LOG_PRECIFICACAO = """
insert into ecode.log_precificacao (idfilial, idfilialsaldo, idgrupopreco, idproduto, idgradex, idgradey, margem, icms, pis, cofins, frete, adicional, customedio, precovenda, regra)
values (%(idfilial)s, %(idfilialsaldo)s, %(idgrupopreco)s, %(idproduto)s, %(idgradex)s, %(idgradey)s, %(margem)s, %(icms)s, %(pis)s, %(cofins)s, %(frete)s, %(adicional)s, %(customedio)s, %(precovenda)s, %(regra)s)
on conflict (idfilial, idfilialsaldo, idgrupopreco, idproduto, idgradex, idgradey) do update set 
(icms, pis, cofins, margem, frete, adicional, customedio, precovenda, regra, created_at) = (excluded.icms, excluded.pis, excluded.cofins, excluded.margem, excluded.frete, excluded.adicional, excluded.customedio, excluded.precovenda, excluded.regra, now())
"""

INSERT_PRODUTOGRADEPRECOGRUPO = """
insert into glb.produtogradeprecogrupo (idproduto, idgradex, idgradey, idgrupopreco, precocusto, precovenda, ultimaalteracao, ultimaremarcacao) 
values (%(idproduto)s, %(idgradex)s, %(idgradey)s, %(idgrupopreco)s, %(precocusto)s, %(precovenda)s, now(), now())
on conflict (idproduto, idgradex, idgradey, idgrupopreco) do update set 
(precocusto, precovenda, ultimaalteracao, ultimaremarcacao) = (excluded.precocusto, excluded.precovenda, now(), now())
"""

SQL_FRETE = """
select
	pf.idgrupopreco,
	pf.classificacao::varchar as classificacao,
	pf.frete,
	row_number() over (order by pf.classificacao::integer desc) as posicao 
from 
	ecode.preco_frete pf 
where 
	pf.situacao = 'Ativo'
	and pf.idgrupopreco = $1
"""

SQL_FRETE_TOTAL = """
select
	pf.idgrupopreco,
	pf.classificacao::varchar as classificacao,
	pf.frete,
	row_number() over (order by pf.classificacao::integer desc) as posicao 
from 
	ecode.preco_frete pf 
where 
	pf.situacao = 'Ativo'
"""

SQL_LOAD_PRECOS = """
select 
	lp.idproduto,
 	lp.idgradex,
  	lp.idgradey,
    lp.precovenda 
from 
	ecode.log_precificacao lp 
where 
	lp.idgrupopreco = %s 
"""

SQL_LOAD_PRECOS_TOTAL = """
select 
  lp.idgrupopreco,
  lp.idproduto,
  lp.idgradex,
  lp.idgradey,
  lp.margem,
  lp.frete,
  lp.icms,
  round(lp.precovenda,2) as precovenda 
from 
  ecode.log_precificacao lp 
where
  coalesce(lp.precovenda,0) > 0
order by idgrupopreco, idproduto , idgradex , idgradey
"""

SQL_LOAD_UF = """
select
   cidade.uf
 from 
   glb.filial filial 
   inner join glb.endereco endereco using (idcnpj_cpf)
   inner join glb.cidade cidade using (idcidade)
 where 
   filial.idfilial = %(idfilial)
   and endereco.idtipoendereco = 1
"""

SQL_CACHE_CUSTO = """

with 

	produto_entrada as (
	
	
		  select 
		    *,
		    case when tmp.idfilialorigem = 10083 then 'Compra-83a' else 'Compra-1a' end as origem_reg
		    
		  from (
		  
		    select itp.idfilialorigem,
		         itp.idregistronota,
		         itp.iditembase,
		         itp.idproduto,
		         itp.idgradex,
		         itp.idgradey,
		         itp.datamovimento,
		         itp.idoperacaoproduto,
		         row_number() over (partition by itp.idfilialorigem, itp.idproduto, itp.idgradex, itp.idgradey order by itp.datamovimento desc) as posicao
		    from rst.itembase itp
		    left join rst.nota ntp
		      on ntp.idfilial = itp.idfilial
		     and ntp.idregistronota = itp.idregistronota
		   where itp.idoperacaoproduto in (101010,101020,101025,101037)
		     and itp.datamovimento <= current_date
		  
		  ) tmp 
		  where posicao = 1
	
	),


	produto as (

		SELECT 
			p.idproduto,
			COALESCE(p.idcest, cnc.idcest) AS idcest,
			a.mva_ajustado, 
		    a.mva_ajustado_import
		FROM 
			glb.produto p
			left join sis.codigonbmcest cnc on cnc.idcodigonbm = p.idcodigonbm
			left join mm.cadastro_mva a on a.idcest = COALESCE(p.idcest, cnc.idcest) and a.datavalidade >= current_date
		WHERE 
			cnc.datainiciovigencia <= CURRENT_DATE
		ORDER BY 
			cnc.datainiciovigencia desc nulls last,
			a.datavalidade DESC
	
	),
	filial as (
	
		select
			filial.idfilial,
		    edr.idtipoendereco,
		    edr.idcidade,
		    cid.cidade,
		    cid.uf as uf_destino,
		    bfi.aliquota as aliquota_icms_interna
		from 
			glb.filial filial 
			left join glb.endereco edr on filial.idcnpj_cpf = edr.idcnpj_cpf and edr.idtipoendereco = 1
			left join glb.cidade cid on cid.idcidade = edr.idcidade 
			left join glb.basefiscaluf bfi on bfi.datafinal = '01/01/2200' and bfi.idtipoimposto = 1 and bfi.uforigem = cid.uf and bfi.uforigem = bfi.ufdestino
		where 
			filial.idfilial in (10001,10050,10083)	
			
	        
	
	),
	
	simulador as (
	
	
	 SELECT a.idproduto,
         a.idgradex,
         a.idgradey,
         a.datamovimento,
         a.idmovimento,
         a.mva_venda_pr,
         a.mva_recalculo,
         coalesce(a.precocusto_mm,0) as precocusto_mm,
         coalesce(a.vlr_icms_st_recup_unit,0) as vlr_icms_st_recup_unit,
         coalesce(a.aliq_icms_pr,0) as aliq_icms_pr,
         a.datahoraatualizacao
    from mm.preco_custo_simulador_aux1 a
   where 
     a.datahoraatualizacao <= to_char(CURRENT_DATE, 'dd/mm/yyyy HHMMSS')
   order by
         a.datamovimento desc,
         a.idmovimento desc,
         a.datahoraatualizacao desc

	
	),
	compra_83a as (
	
	  select
             tb3.origem_reg,
                           
                       
                        tb3.idfilial,
                        
				 		tb3.idproduto, 
				 		tb3.idgradex, 
				 		tb3.idgradey, 
              

                public.roundto((tb3.vlr_icms_st_bruta / (case when tb3.quantidade = 0 then 1 else tb3.quantidade end)),2) as vlr_icms_st_recup_calc,

          
                                                            
                public.roundto( (((coalesce(tb3.totalitem,0)+coalesce(tb3.totalipicusto,0)+coalesce(tb3.totalfretecusto,0)+coalesce(tb3.vlr_icms_st_bruta,0))-
                                   coalesce(tb3.totalpiscofinscusto,0) - coalesce(tb3.valoricms,0)) / tb3.quantidade) ,2) as custo_calc_unit

                from
                 (
                 select
                 tb2.origem_reg,
                           
                       
                        tb2.idfilial,
                        
				 		tb2.idproduto, 
				 		tb2.idgradex, 
				 		tb2.idgradey, 
                 	public.roundto( ((tb2.bc_icms_st_calc * tb2.aliquota_icms_interna) / 100),2) as vlr_icms_st_bruta,
					tb2.quantidade,
	                 tb2.totalitem,
	                 tb2.totalipicusto,
	                 tb2.totalfretecusto,
	                 (case when coalesce(tb2.valoricms,0) > 0
	                          then public.roundto((((tb2.totalitem - coalesce(tb2.valoricms,0)) * tb2.percentualpiscofinscusto)/100),2)
	                          else 0
	                     end) as totalpiscofinscusto,
	                     tb2.valoricms
                    
                    from
                     (
                     
                     select
                      tb1.origem_reg,
                        tb1.idfilial,
				 		tb1.idproduto, 
				 		tb1.idgradex, 
				 		tb1.idgradey, 
                  		(case 
	                  		when tb1.mva = 0 then 0
                            else (case when tb1.idcest is not null
                                         then public.roundto( (((tb1.totalitem + tb1.totalipicusto + tb1.vlripifrete + tb1.totalfretecusto) * (100+tb1.mva))/100),2)
                                         else 0
                                    end)
                         end) as bc_icms_st_calc,
                         tb1.aliquota_icms_interna,
                         tb1.quantidade,
                        tb1.totalitem,
                        tb1.totalipicusto,
                        tb1.totalfretecusto,
                        tb1.valoricms,
                        tb1.percentualpiscofinscusto
                        

                        from
                         (
                         
                         
                         
                         
                         
					select 
                    	 distinct on (3,4,5)     
                         'Compra-83a' as  origem_reg,
                        10083 as idfilial,
                        --itb.idfilial,
				 		itb.idproduto, 
				 		itb.idgradex, 
				 		itb.idgradey, 
				 		itb.datamovimento,
				 		itb.idmovimento,
			         	itb.quantidade,
			         	case when itb.quantidade <> 0
			                 then (itb.totalcustomedio / itb.quantidade)
			                 else itb.totalcustomedio 
			            end as customedio,
			            tbm.idcest,
			            coalesce(itb.totalitem,0) as totalitem,
			            (case when itb.idoperacaoproduto<102000
			                  then public.roundto(((itb.totalitem + itb.totalimpostoimportacao + case when coalesce(itb.somarfreteipi,0)=1 then itb.totalfreteedespesa else 0 end) * itb.aliquotaipi / 100),2)
			                  else 0
			             end) as totalipicusto,
			             0 as vlripifrete, 
			             (case when itb.idoperacaoproduto<102000
			                  then (case
			                          when itb.idcodigofiscal not between 3000 and 3999
			                             then public.roundto(itb.totalfretecusto,2)
			                          when (itb.totaldespesasacessorias - (itb.totalpis + itb.totalcofins)) > 0
			                             then public.roundto(itb.totalfretecusto + itb.totaldespesasacessorias - (itb.totalpis + itb.totalcofins),2)
			                          else 0
			                        end)
			                  else 0
			             end) as totalfretecusto,
			             rfi.aliquota_icms_interna,
			     				             
				            (case when tbx.mva_recalculo < 0
                                  then 0
			                          when itb.aliquotaicms in (12,19) then tbm.mva_ajustado
			                          when itb.aliquotaicms = 4  then tbm.mva_ajustado_import
			                          else 0
			                        end) as mva,
			                        public.roundto((itb.totalbasecalculoicms * itb.aliquotaicms / 100) - 
                                           (itb.totalbasecalculoicms * itb.aliquotaicms / 100 * itb.percentualdiferido / 100),2) as valoricms,
                                           public.roundto((((coalesce(itb.totalbasecalculopis,0)    - coalesce(itb.totalfreteedespesa,0)) * coalesce(itb.aliquotapis,0) / 100) +
                                            ((coalesce(itb.totalbasecalculocofins,0) - coalesce(itb.totalfreteedespesa,0)) * coalesce(itb.aliquotacofins,0) / 100)),2) AS totalpiscofinscusto,
                            (coalesce(itb.aliquotapis,0) + coalesce(itb.aliquotacofins,0)) AS percentualpiscofinscusto,
			             	row_number() over (partition by /*itb.idfilial, */itb.idproduto, itb.idgradex, itb.idgradey order by itb.datamovimento desc, itb.idmovimento desc) as posicao
                            from rst.itembase itb
			                left join sis.operacao o on itb.idoperacaoproduto = o.idoperacao
			                left join glb.filial fil on fil.idfilial = itb.idfilial
			                left join glb.produto p on itb.idproduto = p.idproduto
			                left join rst.itempedidocompraitembase ipc on ipc.idfilial = itb.idfilial and ipc.iditembase = itb.iditembase
			                left join filial rfi on rfi.idfilial = fil.idfilial
			                left join produto tbm on tbm.idproduto = itb.idproduto
                            LEFT JOIN simulador as tbx on tbx.idproduto = itb.idproduto and tbx.idgradex = itb.idgradex and tbx.idgradey = itb.idgradey
                            inner join produto_entrada entrada 
                            	on entrada.idproduto = itb.idproduto 
                            	and entrada.idgradex = itb.idgradex 
                            	and entrada.idgradey = itb.idgradey
                            	and entrada.origem_reg = 'Compra-83a'
                           where itb.idfilial in (10001,10050,10083,10132)
                             and itb.idoperacaoproduto in (101010)
                             and itb.datamovimento <= current_date
                         ) tb1
                         where posicao = 1

                      ) tb2

                  ) tb3
	
	)
	,
	Compra_1a as (
	
	
		select 
		a.origem_reg,		
			a.idfilial,
		  a.idproduto, 
		    a.idgradex, 
			a.idgradey, 
	     	round(a.customedio,2) as custo_calc_unit,
			round((((a.bc_icms_st_calc * a.aliquota_icms_interna) / 100) / (case when a.quantidade = 0 then 1 else a.quantidade end)),2) as vlr_icms_st_recup_calc
		from (
		
				select 
				  dados.origem_reg,
				  dados.idfilial,
				  dados.idproduto, 
				  dados.idgradex, 
				  dados.idgradey, 
				  dados.quantidade,
			     	dados.customedio,
				 	(case when dados.idcest is not null
				          then public.roundto( (((dados.totalitem + dados.totalipicusto + dados.vlripifrete + dados.totalfretecusto) * (100+dados.mva))/100),2)
				          else 0
				     end) as bc_icms_st_calc,
				     dados.aliquota_icms_interna 
				from 
				  (
				  
				  select 
                         	
                       distinct on (itb.idproduto, itb.idgradex, itb.idgradey )
                        'Compra-1a' as  origem_reg,
                             
                        itb.idfilial,
				 		itb.idproduto, 
				 		itb.idgradex, 
				 		itb.idgradey, 
				 		itb.datamovimento,
				 		itb.idmovimento,
			         	itb.quantidade,
			         	case when itb.quantidade <> 0
			                 then (itb.totalcustomedio / itb.quantidade)
			                 else itb.totalcustomedio 
			            end as customedio,
			            tbm.idcest,
			            coalesce(itb.totalitem,0) as totalitem,
			            (case when itb.idoperacaoproduto<102000
			                  then public.roundto(((itb.totalitem + itb.totalimpostoimportacao + case when coalesce(itb.somarfreteipi,0)=1 then itb.totalfreteedespesa else 0 end) * itb.aliquotaipi / 100),2)
			                  else 0
			             end) as totalipicusto,
			             0 as vlripifrete, 
			             (case when itb.idoperacaoproduto<102000
			                  then (case
			                          when itb.idcodigofiscal not between 3000 and 3999
			                             then public.roundto(itb.totalfretecusto,2)
			                          when (itb.totaldespesasacessorias - (itb.totalpis + itb.totalcofins)) > 0
			                             then public.roundto(itb.totalfretecusto + itb.totaldespesasacessorias - (itb.totalpis + itb.totalcofins),2)
			                          else 0
			                        end)
			                  else 0
			             end) as totalfretecusto,
			             rfi.aliquota_icms_interna,
			             (case
			                          when itb.aliquotaicms in (12,19) then tbm.mva_ajustado
			                          when itb.aliquotaicms = 4  then tbm.mva_ajustado_import
			                          else 0
			                        end) as mva,
			             row_number() over (partition by itb.idfilial, itb.idproduto, itb.idgradex, itb.idgradey order by itb.datamovimento desc, itb.idmovimento desc) as posicao
                            
						from rst.itembase itb
			                left join sis.operacao o on itb.idoperacaoproduto = o.idoperacao
			                left join glb.filial fil on fil.idfilial = itb.idfilial
			                left join glb.produto p on itb.idproduto = p.idproduto
			                left join rst.itempedidocompraitembase ipc on ipc.idfilial = itb.idfilial and ipc.iditembase = itb.iditembase
			                left join filial rfi on rfi.idfilial = fil.idfilial
			                left join produto tbm on tbm.idproduto = itb.idproduto
			                inner join produto_entrada entrada 
                            	on entrada.idproduto = itb.idproduto 
                            	and entrada.idgradex = itb.idgradex 
                            	and entrada.idgradey = itb.idgradey
                            	and entrada.origem_reg = 'Compra-1a'
                           where itb.idoperacaoproduto in (101010,101020,101025,101037)
                             and itb.idfilial       = 10001
                             and itb.datamovimento <= current_date
                             and itb.idlocalsaldo   = 1
				  
				  
				  ) dados
				  where posicao = 1
				
		) a
	),
	atualizacao as (
	
	insert into ecode.preco_customedio (origem_reg,idfilial,idproduto,idgradex,idgradey,custo_calc_unit,vlr_icms_st_recup_calc)
	select origem_reg, idfilial, idproduto, idgradex, idgradey, custo_calc_unit, vlr_icms_st_recup_calc  from Compra_83a	
	union
	select origem_reg, idfilial, idproduto, idgradex, idgradey, custo_calc_unit, vlr_icms_st_recup_calc from Compra_1a 
	on conflict (idfilial,idproduto,idgradex,idgradey) 
	do update set 
		custo_calc_unit = excluded.custo_calc_unit,
		vlr_icms_st_recup_calc = excluded.vlr_icms_st_recup_calc,
		created_at = now()
	returning 1
	)
	select count(1) from atualizacao 
"""

SQL_UPDATE_CACHE_SALDO_FUNCAO = """
	insert into ecode.preco_customedio
	select 
		custo_calc_unit,
		vlr_icms_st_recup_calc,
		origem_reg
	from 
 		mm.busca_ultima_entrada_com_icms_itb_teste(
        %(idfilial)s,
        %(idproduto)s,
        %(idgradex)s,
        %(idgradey)s,
        current_date,
        999999999)
	on 
 		conflict(idfilial, idproduto, idgradex, idgradey) 
	do 
 		update set 
		custo_calc_unit = excluded.custo_calc_unit,
		vlr_icms_st_recup_calc = excluded.vlr_icms_st_recup_calc,
		origem_reg = excluded.origem_reg
"""

SQL_LOAD_PRODUTO_FILIAL = """


 with produtos as (
  
  select 
    p.idfilial,
    p.idproduto,
    p.idgradex,
    p.idgradey,
    sum(p.saldo * t.multiplicadordisponivel) as saldo 
  from 
    rst.produtogradesaldofilial p 
    left join sis.tiposaldoproduto t using (idtiposaldoproduto)
  where 
    p.idfilial = %(idfilial)s
  group by 
    1,2,3,4
  having 
    sum(p.saldo * t.multiplicadordisponivel) > 0
    
    union 
    
    select 
      i.idfilial,
      i.idproduto,
      i.idgradex,
      i.idgradey,
      i."quantidade" as saldo
    from 
      rst.itempedidocompra i 
    where 
      i.idfilial = %(idfilial)s
      and idsituacaopedidocompra = 1
  
  ),
  
  custo_existente as (
	
    select 
      pc.idfilial,
      pc.idproduto,
      pc.idgradex,
      pc.idgradey,
      pc.custo_calc_unit,
	  pc.vlr_icms_st_recup_calc,
	  pc.vlr_icms_proprio_entrada_unit
	from 
      ecode.preco_customedio pc 
    where 
      pc.idfilial = %(idfilial)s
   
  )

  select 
    distinct on (1,2,3,4)
	produtogradefilial.idfilial,
	produtogradefilial.idproduto,
	produtogradefilial.idgradex,
	produtogradefilial.idgradey,
	coalesce(ce.custo_calc_unit,0) as custo_calc_unit,
    coalesce(ce.vlr_icms_st_recup_calc,0) as vlr_icms_st_recup_calc,
    coalesce(ce.vlr_icms_proprio_entrada_unit,0) as vlr_icms_proprio_entrada_unit
from 
	rst.produtogradefilial produtogradefilial
	inner join produtos saldo using (idfilial, idproduto,idgradex, idgradey)
	inner join glb.produto produto using (idproduto)
    left join custo_existente ce using (idfilial, idproduto,idgradex, idgradey)
where 
	produtogradefilial.idfilial = %(idfilial)s
	and substring(produto.iddepartamento::varchar,2,2) in ('01','02','03','04','05','06','07','08','09','13','15','19')
order by 
    1,2,3,4  
"""

SQL_GET_CUST_MEDIO = """
select 
    round(custo_medio.custo_calc_unit,2) as custo_calc_unit,
	round(custo_medio.vlr_icms_st_recup_calc,2) as vlr_icms_st_recup_calc,
	(CASE 
        WHEN cast(custo_medio.idfilial  as integer) = 10281
		THEN (CASE WHEN coalesce(custo_medio.qtdenf,0) > 0
					THEN round((coalesce(custo_medio.vlr_icms_proprio,0)/coalesce(custo_medio.qtdenf,0)),2)
					ELSE round((coalesce(custo_medio.vlr_icms_proprio,0)/ 1),2)
				END)
		ELSE 0
	END) as vlr_icms_proprio_entrada_unit,
	custo_medio.origem_reg
from 
 	mm.busca_ultima_entrada_com_icms_itb_teste(
	  case when %(idfilial)s in (10001,10083) then 10050 else %(idfilial)s end,
	  %(idproduto)s,
	  %(idgradex)s,
	  %(idgradey)s,
	  current_date,
	  999999999) as custo_medio
"""
SQL_GET_FILIAIS_PRECIFICAR = """
select distinct idfilialsaldo from ecode.preco_base where situacao = 'Ativo' order by idfilialsaldo desc
"""

SQL_GET_CUST_MEDIO_RESERVA = """
select 
  tmp.custo_calc_unit,
  tmp.vlr_icms_st_recup_calc,
  tmp.origem_reg
from (
	SELECT 
	  (a.precocusto * '1.05') as custo_calc_unit,
	  0 as vlr_icms_st_recup_calc,
	  'produtogradeprecogruporemarcacao' as origem_reg,
	  row_number() over (order by a.dataremarcacao DESC) as posicao
	FROM 
	  glb.produtogradeprecogruporemarcacao a
	WHERE 
	  a.idproduto = :idproduto
	  AND a.idgradex  = :idgradex
	  AND a.idgradey  = :idgradey
	  AND a.dataremarcacao >= '01/01/2024'
) tmp 
where 
  tmp.posicao = 1
"""

SQL_GET_CUST_MEDIO_REMARCACAO = """
select 
  tmp.idproduto,
  tmp.idgradex,
  tmp.idgradey,
  tmp.custo_calc_unit,
  tmp.vlr_icms_st_recup_calc,
  tmp.origem_reg
from (
	SELECT 
	  a.idproduto,
	  a.idgradex,
	  a.idgradey,
	  (a.precocusto * '1.05') as custo_calc_unit,
	  0 as vlr_icms_st_recup_calc,
	  'produtogradeprecogruporemarcacao' as origem_reg,
	  row_number() over (partition by a.idproduto, a.idgradex, a.idgradey order by a.dataremarcacao DESC) as posicao
	FROM 
	  glb.produtogradeprecogruporemarcacao a
	WHERE 
	  a.dataremarcacao >= '01/01/2024'
) tmp 
where 
  tmp.posicao = 1"""

SQL_PERSISTENCIA_CUSTO = """
insert into 
  ecode.preco_customedio 
  (idfilial, idproduto, idgradex, idgradey, custo_calc_unit, vlr_icms_st_recup_calc, vlr_icms_proprio_entrada_unit, origem_reg) 
values
  (%(idfilial)s, %(idproduto)s, %(idgradex)s, %(idgradey)s, %(custo_calc_unit)s, %(vlr_icms_st_recup_calc)s, %(vlr_icms_proprio_entrada_unit)s, %(origem_reg)s)
on conflict 
  (idfilial, idproduto, idgradex, idgradey)
do update set
  custo_calc_unit = excluded.custo_calc_unit,
  vlr_icms_st_recup_calc = excluded.vlr_icms_st_recup_calc, 
  vlr_icms_proprio_entrada_unit = excluded.vlr_icms_proprio_entrada_unit, 
  origem_reg = excluded.origem_reg,
  created_at = now()
"""

SQL_UPSERT_CUSTOMEDIO="""

with customedio as (

select 
    %(idfilial)s as idfilial,
    %(idproduto)s as idproduto, 
    %(idgradex)s as idgradex, 
    %(idgradey)s as idgradey, 
    round(custo_medio.custo_calc_unit,2) as custo_calc_unit,
	round(custo_medio.vlr_icms_st_recup_calc,2) as vlr_icms_st_recup_calc,
	(CASE 
        WHEN cast(custo_medio.idfilial  as integer) = 10281
		THEN (CASE WHEN coalesce(custo_medio.qtdenf,0) > 0
					THEN round((coalesce(custo_medio.vlr_icms_proprio,0)/coalesce(custo_medio.qtdenf,0)),2)
					ELSE round((coalesce(custo_medio.vlr_icms_proprio,0)/ 1),2)
				END)
		ELSE 0
	END) as vlr_icms_proprio_entrada_unit,
	custo_medio.origem_reg
from 
 	mm.busca_ultima_entrada_com_icms_itb_teste(
	  case when %(idfilial)s in (10001,10083) then 10050 else %(idfilial)s end,
	  %(idproduto)s,
	  %(idgradex)s,
	  %(idgradey)s,
	  current_date,
	  999999999) as custo_medio

),
persistencia as (


	insert into 
	  ecode.preco_customedio 
	  (idfilial, idproduto, idgradex, idgradey, custo_calc_unit, vlr_icms_st_recup_calc, vlr_icms_proprio_entrada_unit, origem_reg) 
	
	select 
	  customedio.* 
	from 
	  customedio customedio
	  left join ecode.preco_customedio preco_customedio on 
	  	customedio.idfilial = preco_customedio.idfilial
	  	and customedio.idproduto = preco_customedio.idproduto
	  	and customedio.idgradex = preco_customedio.idgradex
	  	and customedio.idgradey = preco_customedio.idgradey
	where 
	   (coalesce(customedio.custo_calc_unit,0) <> coalesce(preco_customedio.custo_calc_unit,0)
	   or coalesce(customedio.vlr_icms_st_recup_calc,0) <> coalesce(preco_customedio.vlr_icms_st_recup_calc,0)
	   or coalesce(customedio.vlr_icms_proprio_entrada_unit,0) <> coalesce(preco_customedio.vlr_icms_proprio_entrada_unit,0))
	  
	on conflict 
	  (idfilial, idproduto, idgradex, idgradey)
	do update set
	  custo_calc_unit = excluded.custo_calc_unit,
	  vlr_icms_st_recup_calc = excluded.vlr_icms_st_recup_calc, 
	  vlr_icms_proprio_entrada_unit = excluded.vlr_icms_proprio_entrada_unit, 
	  origem_reg = excluded.origem_reg,
	  created_at = now()
	  
	returning 'atualizado'


)

select coalesce((select * from persistencia),'nao_atualizado') as situacao
"""
