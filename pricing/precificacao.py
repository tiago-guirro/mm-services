"""Módulo de precificação e criação do multi-grupo preço MM."""
from queue import Queue, Empty
from typing import Any
from decimal import Decimal
import psycopg
from psycopg.rows import dict_row
from psycopg_pool import PoolTimeout
from pricing.utils.log import log_error, log_notify
from pricing.pool_conn import pool
from pricing.utils.cache import cache
from pricing.utils.cache_redis import cache as cache_redis
from pricing.utils.calculos import round_salles, round_two, round_up
from pricing.query.atacado_imposto import SQL as sql_imposto
from pricing.sql import (
    SQL_LOAD_REGRA,
    SQL_INIT_TEST,
    INSERT_LOG_PRECIFICACAO,
    INSERT_PRODUTOGRADEPRECOGRUPO,
    SQL_FRETE_TOTAL,
    SQL_LOAD_PRECOS_TOTAL
    )

w_precos = Queue()
w_log = Queue()
regra: list[dict] = []
frete: list[dict] = []
imposto: dict = {}
preco_comparacao = set()
no_duplicate_key = set()

class Operacoes:
    """Pegando dados banco"""
    def __init__(self) -> None:
        self._expires: int | None = 0

    def get_regra(self):
        """Retornando dados regra"""
        try:
            with pool.connection() as conn:
                with conn.cursor(row_factory=dict_row) as cur:
                    cur.execute(SQL_LOAD_REGRA, prepare=False)
                    for c in cur:
                        regra.append(c)
                conn.commit()
        except psycopg.Error as e:
            log_error(f"get_regra {e}")

    def get_impostos(self, idproduto: int):
        """Retornando regra de imposto para atacado"""
        try:
            log_notify(f"Carregando Imposto: {idproduto}")
            with pool.connection() as conn:
                with conn.cursor(row_factory=dict_row) as cur:
                    cur.execute(sql_imposto,
                                {"idproduto" : idproduto},
                                prepare=False)
                    for item in cur:
                        chave = f"imposto:{int(item.get('idfilial',0))}:"
                        chave += f"{int(item.get('idgrupopreco',0))}:"
                        chave += f"{int(item.get('idproduto',0))}"
                        for k_ in ['idproduto','idfilial','idgrupopreco']:
                            del item[k_]
                        cache_redis.set(chave, item, ex=None)
                conn.commit()
        except psycopg.Error as e:
            log_error(f"get_impostos {e}")

    def get_preco_comparacao(self):
        """Retornando dados comparação"""
        if preco_comparacao:
            return
        try:
            with pool.connection() as conn:
                with conn.cursor(row_factory=dict_row) as cur:
                    cur.execute(SQL_LOAD_PRECOS_TOTAL, prepare=False)
                    for p in cur:
                        campos = [p.get('idgrupopreco'),
                                p.get('idproduto'),
                                p.get('idgradex'),
                                p.get('idgradey'),
                                p.get('precovenda'),
                                p.get('margem'),
                                p.get('frete'),
                                round_two(p.get('icms',0)),
                                p.get('pis'),
                                p.get('cofins')]
                        key = '_'.join(str(c) if c is not None else '' for c in campos)
                        preco_comparacao.add(key)
                conn.commit()
        except psycopg.Error as e:
            log_error(f"get_preco_comparacao {e}")

    def get_customedio_ajustado(self, **k) -> list:
        """Custo médio ajustado"""
        @cache.memoize(expire=60*10)
        def get_data(**kk):
            try:
                with pool.connection() as conn:
                    with conn.cursor(row_factory=dict_row) as cur:
                        cur.execute(SQL_INIT_TEST,kk,prepare=False)
                        dados = cur.fetchall()
                    conn.commit()
                return dados
            except psycopg.Error as e:
                log_error(f"get_customedio_ajustado {e} {k}")
                return []
        return get_data(**k)

    def get_frete(self):
        """Load frete de toda rede"""
        
        if frete:
            return
        
        try:
            with pool.connection() as conn:
                with conn.cursor(row_factory=dict_row) as cur:
                    cur.execute(SQL_FRETE_TOTAL, prepare=False)
                    for c in cur:
                        frete.append(c)
                conn.commit()
        except psycopg.Error as e:
            log_error(f"get_frete {e}")

    def insert_many(self):
        """Inserindo lote de dados"""
        try:
            log_notify(f"Iniciando Gravação {w_precos.qsize()}")
            with pool.connection() as conn:
                with conn.cursor() as cur:
                    controle = 0
                    safe = []
                    while not w_precos.empty():
                        try:
                            preco = w_precos.get_nowait()
                            log = w_log.get_nowait()
                            safe.append([preco, log])
                            cur.execute(INSERT_PRODUTOGRADEPRECOGRUPO,preco,prepare=False)
                            cur.execute(INSERT_LOG_PRECIFICACAO,log,prepare=False)
                            controle += 1
                            if controle % 150 == 0:
                                conn.commit()
                                for _ in range(0,controle):
                                    w_precos.task_done()
                                    w_log.task_done()
                                log_notify(f"Gravando {controle} {w_precos.qsize()}")
                                safe.clear()
                                controle = 0
                        except Empty:
                            break
                conn.commit()
                for _ in range(0,controle):
                    w_precos.task_done()
                    w_log.task_done()
                    log_notify(f"Gravando {controle} {w_precos.qsize()}")
                safe.clear()
        except (PoolTimeout,
                psycopg.errors.DuplicatePreparedStatement,
                psycopg.Error) as e:
            log_error(f"Error {e}")
            for s in safe:
                w_precos.put(s[0])
                w_log.put(s[1])
            safe.clear()

class Precificacao:
    """Precificacao"""
    def __init__(self):
        """Inicialdo conexão"""
        self.ops = Operacoes()
        self.ops.get_regra()
        log_notify(f"Regra Carregada. {len(regra)}")
        self.ops.get_preco_comparacao()
        log_notify(f"Preço Comparação Carregada. {len(preco_comparacao)}")
        self.ops.get_frete()
        log_notify(f"Frete Carregado. {len(frete)}")
        self._set_precificacao()

    def get_frete_search(self, idgrupopreco, classificacao):
        """Coletando no cache os dados por produto"""
        @cache.memoize(expire=60*10)
        def get_data(_idgrupopreco, _classificacao):
            for f in frete:
                if not f.get('idgrupopreco') == _idgrupopreco:
                    continue
                if _classificacao.startswith(f.get('classificacao'),0):
                    _cache = f.get('frete',0)
                    return _cache
            return 0
        return get_data(idgrupopreco, classificacao)

    def get_calc_sales_price(self, idproduto: int, customedio: Decimal, rl: dict) -> Decimal | None:
        """Coletando no cache os dados por produto"""
        chave = f"imposto:{int(rl.get('idfilial',0))}:{int(rl.get('idgrupopreco',0))}:{idproduto}"
        if not cache_redis.exists(chave):
            self.ops.get_impostos(idproduto)
            if not cache_redis.exists(chave):
                return None
        impostos: Any = cache_redis.get(chave)
        icms_ = Decimal(impostos.get('icms_origem',0))
        if Decimal(impostos.get('percentualdiferido',0)) > Decimal(0):
            diferido_ = Decimal(100) - Decimal(impostos.get('percentualdiferido',100))
            diferido_ /= Decimal(100)
            icms_ *= diferido_
        pis_ = Decimal(impostos.get('pis',0))
        pis_ -= pis_ * icms_ / Decimal(100)
        cofins_ = Decimal(impostos.get('cofins',0))
        cofins_ -= cofins_ * icms_ / Decimal(100)
        rl.update({"icms" : round_two(icms_)})
        rl.update({"pis" : round_up(pis_)})
        rl.update({"cofins" : round_up(cofins_)})
        campos = ['icms', 'margem', 'adicional', 'frete', 'pis', 'cofins']
        _idx = sum(Decimal(rl.get(campo, 0)) for campo in campos)
        return round_salles(customedio / (Decimal(1) - (Decimal(_idx) / Decimal(100))))

    def _set_precificacao(self):
        """Criando precificação"""
        totalizador: int = 1
        for rul_o in regra:
            # Listando regras por ordem de importancia
            if not rul_o.get('idfilialsaldo'):
                continue
            custos = self.ops.get_customedio_ajustado(
                idfilial = rul_o.get('idfilialsaldo'),
                ncm = rul_o.get('ncm'),
                classificacao = rul_o.get('classificacao'),
                origem = rul_o.get('origem'),
                idmarca = rul_o.get('idmarca'),
                idproduto = rul_o.get('idproduto'),
                idgradex = rul_o.get('idgradex'),
                idgradey = rul_o.get('idgradey')
            )
            for custo in custos:
                rul = rul_o.copy()
                _frete:float = 0
                key: str = f"{rul.get('idgrupopreco')}_"
                key += f"{custo.get('idproduto')}_"
                key += f"{custo.get('idgradex')}_"
                key += str(custo.get('idgradey'))
                if key in no_duplicate_key:
                    continue
                no_duplicate_key.add(key)

                # Verificando o formato de agrupamento por idproduto.idgradex.idgradey ou idproduto
                _customedio = custo.get('customedio')
                if rul.get('agrupar_x_y', 'Não') == 'Sim':
                    _customedio = custo.get('customedio_agrupado')
                _frete = self.get_frete_search(rul.get('idgrupopreco',0),
                                               custo.get('classificacao','0'))
                if _frete > 0:
                    rul.update({'frete' : _frete})
                price: Decimal | None = self.get_calc_sales_price(custo.get('idproduto'),
                                                                  _customedio,rul)
                if not price:
                    continue
                # Validando preço zero ou igual customedio
                if price <= _customedio or price <= 0:
                    raise ValueError('Custo abaixo do permitido')
                key += f"_{price}_"
                key += f"{Decimal(rul.get('margem',0))}_"
                key += f"{Decimal(rul.get('frete',0))}_"
                key += f"{Decimal(rul.get('icms',0))}_"
                key += f"{Decimal(rul.get('pis',0))}_"
                key += f"{Decimal(rul.get('cofins',0))}"

                if key in preco_comparacao:
                    continue

                totalizador += 1
                w_log.put(
                    {
                        "idfilial": rul.get('idfilial'),
                        "idfilialsaldo": rul.get('idfilialsaldo'),
                        "idgrupopreco": rul.get('idgrupopreco'),
                        "idproduto": custo.get('idproduto'),
                        "idgradex": custo.get('idgradex'),
                        "idgradey": custo.get('idgradey'),
                        "margem": rul.get('margem'),
                        "icms": rul.get('icms'),
                        "pis": rul.get('pis'),
                        "cofins": rul.get('cofins'),
                        "frete": rul.get('frete'),
                        "adicional": rul.get('adicional'),
                        "customedio": _customedio,
                        "precovenda": price,
                        "regra": f"{rul.get('id_base')}-{rul.get('regra')}"
                    }
                )
                w_precos.put(
                    {
                        "idproduto": custo.get('idproduto'),
                        "idgradex": custo.get('idgradex'),
                        "idgradey": custo.get('idgradey'),
                        "precocusto": _customedio,
                        "idgrupopreco": rul.get('idgrupopreco'),
                        "precovenda": price

                    }
                )
            log_notify(f"Total {rul_o.get('idgrupopreco',0)} {w_precos.qsize()}")
        if w_precos.qsize() > 0:
            self.ops.insert_many()

    def __del__(self):
        """del conexão"""
        regra.clear()
        frete.clear()
        preco_comparacao.clear()
        no_duplicate_key.clear()
