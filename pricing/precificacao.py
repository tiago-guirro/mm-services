"""Módulo de precificação e criação do multi-grupo preço MM."""
# from datetime import datetime
import math
from decimal import Decimal
from diskcache import Cache
import psycopg
from psycopg.rows import dict_row
from pricing.sql import (
    SQL_LOAD_REGRA,
    SQL_INIT_TEST,
    INSERT_LOG_PRECIFICACAO,
    INSERT_PRODUTOGRADEPRECOGRUPO,
    SQL_FRETE_TOTAL,
    SQL_LOAD_PRECOS_TOTAL
    )

class Precificacao:
    """Precificacao"""
    URL = '/home/ecode/Python/MM/cache'

    def __init__(self, pool, capture_exception, logger):
        """Inicialdo conexão"""
        self.non_chance_price: list = []
        self.pool = pool
        self.capture_exception = capture_exception
        self.logger = logger
        self.logger.info("Precificacao")
        self.cache = Cache(self.URL)
        self.cache.clear()
        self.__lote_persist()

    def setting_error(self, local, error) -> None:
        """setting_error"""
        self.logger.error(f"{str(error)} {local}")
        self.capture_exception(error)

    def load_frete(self) -> dict | None:
        """Load frete de toda rede"""
        if 'Frete' in self.cache:
            return self.cache.get('Frete')
        try:
            with self.pool.connection() as conn:
                with conn.cursor(row_factory=dict_row) as cur:
                    with conn.transaction():
                        _cache = cur.execute(SQL_FRETE_TOTAL, prepare=False).fetchall()
                        self.cache.set('Frete',_cache)
                        return _cache
        except psycopg.Error as e:
            self.setting_error('load_frete', e)
            return None

    def get_frete_search(self, idgrupopreco, classificacao):
        """Coletando no cache os dados por produto"""
        _key_frete_search = f"frete_search_{idgrupopreco}_{classificacao}"
        if _key_frete_search in self.cache:
            return self.cache.get(_key_frete_search)
        frete = self.load_frete()
        if not frete:
            return 0
        for f in frete:
            if not f.get('idgrupopreco') == idgrupopreco:
                continue
            if classificacao.startswith(f.get('classificacao'),0):
                _cache = f.get('frete',0)
                self.cache.set(_key_frete_search,_cache)
                return _cache
        return 0

    def load_preco(self):
        """Carregamento de preço"""
        _key_load = "load_preco"
        if _key_load in self.cache:
            return self.cache.get(_key_load)
        try:
            with self.pool.connection() as conn:
                with conn.cursor(row_factory=dict_row) as cur:
                    with conn.transaction():
                        _cache = cur.execute(SQL_LOAD_PRECOS_TOTAL, prepare=False).fetchall()
                        self.cache.set(_key_load,_cache)
                        return _cache
        except psycopg.Error as e:
            self.setting_error('load_preco', e)
            return None

    def get_preco_cache(self, idgrupopreco: int, idproduto: int, idgradex: int, idgradey: int):
        """Coletando no cache os dados por produto"""
        _key = f"preco_cache_{idgrupopreco}_{idproduto}_{idgradex}_{idgradey}"
        if _key in self.cache:
            return self.cache.get(_key)
        precos = self.load_preco()
        if not precos:
            return 0
        for x in precos:
            if (x.get('idproduto') == idproduto and
                x.get('idgradex') == idgradex and
                x.get('idgradey') == idgradey and
                x.get('idgrupopreco') == idgrupopreco
                ):
                _cache = float(x.get('precovenda',0))
                self.cache.set(_key,_cache)
                return _cache
        return 0

    def get_customedio(self, idfilial: int):
        """Coletando no cache os dados por produto"""
        _key = f"customedio_{idfilial}"
        if _key in self.cache:
            return self.cache.get(_key)
        params:dict = {}
        params.update({'idfilial' : idfilial})
        try:
            with self.pool.connection() as conn:
                with conn.cursor(row_factory=dict_row) as cur:
                    with conn.transaction():
                        cur.execute(SQL_INIT_TEST, params, prepare=False)
                        _cache = cur.fetchall()
        except psycopg.Error as e:
            self.setting_error('get_customedio', e)
            return None
        self.cache.set(_key,_cache)
        return _cache

    def get_calc_sales_price(self, customedio: Decimal, rl: dict, idsituacaoorigem: str):
        """Coletando no cache os dados por produto"""
        _desoneracao: float = ((100 - rl.get('icms',0)) / 100)
        _pis: float  = rl.get('pis',0) * _desoneracao
        _cofins: float = rl.get('cofins',0) * _desoneracao
        _icms: Decimal = (Decimal(4.0) if rl.get('interestadual','Não') == 'Sim'
                and idsituacaoorigem in (1,2,3,8) else rl.get('icms',0))
        # Alterando para o log o novo icms
        rl.update({"icms" : _icms})
        _idx: float = (
            _icms +
            rl.get('margem',0) +
            rl.get('adicional',0) +
            rl.get('frete',0) +
            _pis +
            _cofins
            )
        return math.trunc(customedio / (1 - (_idx / 100))) + 0.9

    def insert_many(self, sql, bigdata) -> None:
        """Inserindo lote de dados"""
        if not bigdata:
            return
        try:
            with self.pool.connection() as conn:
                conn.prepare_threshold=1000
                conn.commit()
                with conn.cursor() as cur:
                    with conn.transaction():
                        cur.executemany(sql, bigdata)
                        self.logger.info("insert_many " + str(len(bigdata)))
        except psycopg.Error as e:
            self.setting_error('insert_many', e)
            self.insert_many(sql, bigdata)

    def __no_duplicate_key(self, **k) -> bool:
        """Gerando key para não duplicidade de cadastro"""
        key = str(
            str(k.get('idproduto')).zfill(8) +
            str(k.get('idgradex')).zfill(6) +
            str(k.get('idgradey')).zfill(6) +
            str(k.get('idgrupopreco')).zfill(6)
        )
        if key in self.non_chance_price:
            return True
        self.non_chance_price.append(key)
        return False

    def __set_precificacao(self):
        """Criando precificação"""
        try:
            with self.pool.connection() as conn:
                with conn.cursor(row_factory=dict_row) as cur:
                    with conn.transaction():
                        rules = cur.execute(SQL_LOAD_REGRA, prepare=False).fetchall()
        except psycopg.Error as e:
            self.setting_error('get_customedio', e)
            return None

        totalizador: int = 1

        log_pool: list = []
        log_preco: list = []

        for rul in rules:
            # Listando regras por ordem de importancia
            custos = self.get_customedio(rul.get('idfilialsaldo',0))

            for custo in custos:
                rule = rul.copy()
                _frete:float = 0

                # Regra ncm
                if (rule.get('ncm') and
                    rule.get('ncm') != custo.get('idcodigonbm')):
                    continue
                # Regra produto
                if (rule.get('idproduto') and
                    (rule.get('idproduto') != custo.get('idproduto') and
                    rule.get('idgradex') != custo.get('idgradex') and
                    rule.get('idgradey') != custo.get('idgradey')
                    )):
                    continue
                # Regra marca
                if (rule.get('idmarca') and
                    rule.get('idmarca') != custo.get('idmarca')):
                    continue
                # Regra origem
                if (rule.get('origem') and
                    rule.get('origem') != custo.get('idsituacaoorigem')):
                    continue
                # Regra classificacao
                if (rule.get('classificacao') and not
                    custo.get('classificacao').startswith(rule.get('classificacao'),0)):
                    continue
                if self.__no_duplicate_key(
                    idproduto=custo.get('idproduto'),
                    idgradex=custo.get('idgradex'),
                    idgradey=custo.get('idgradey'),
                    idgrupopreco=rule.get('idgrupopreco')
                    ):
                    continue

                # Verificando o formato de agrupamento por idproduto.idgradex.idgradey ou idproduto
                _customedio = custo.get('customedio_agrupado') if rule.get(
                    'agrupar_x_y', 'Não') == 'Sim' else custo.get('customedio')
                _frete = self.get_frete_search(
                    rule.get('idgrupopreco'), custo.get('classificacao'))
                if _frete > 0:
                    rule.update({'frete' : _frete})
                price = self.get_calc_sales_price(
                    _customedio, rule, custo.get('idsituacaoorigem', 0))

                # Validando preço zero ou igual customedio
                if price <= _customedio or price <= 0:
                    raise ValueError('Custo abaixo do permitido')

                price_now = self.get_preco_cache(
                    rule.get('idgrupopreco'),
                    custo.get('idproduto'),
                    custo.get('idgradex'),
                    custo.get('idgradey')
                    )

                # Verificando se existe o mesmo preço
                if price_now == price:
                    continue

                totalizador += 1

                _log:dict = {}
                _log.update({"idfilial" : rule.get('idfilial')})
                _log.update({"idfilialsaldo" : rule.get('idfilialsaldo')})
                _log.update({"idgrupopreco" : rule.get('idgrupopreco')})
                _log.update({"idproduto" : custo.get('idproduto')})
                _log.update({"idgradex" : custo.get('idgradex')})
                _log.update({"idgradey" : custo.get('idgradey')})
                _log.update({"margem" : rule.get('margem')})
                _log.update({"icms" : rule.get('icms')})
                _log.update({"pis" : rule.get('pis')})
                _log.update({"cofins" : rule.get('cofins')})
                _log.update({"frete" : rule.get('frete')})
                _log.update({"adicional" : rule.get('adicional')})
                _log.update({"customedio" : _customedio})
                _log.update({"precovenda" : price})
                _log.update({"regra" : rule.get('regra')})
                _preco:dict = {}
                _preco.update({"idproduto" : custo.get('idproduto')})
                _preco.update({"idgradex" : custo.get('idgradex')})
                _preco.update({"idgradey" : custo.get('idgradey')})
                _preco.update({"precocusto" : _customedio})
                _preco.update({"idgrupopreco" : rule.get('idgrupopreco')})
                _preco.update({"precovenda" : price})

                log_pool.append(_log)
                log_preco.append(_preco)

                if totalizador % 1000 == 0:
                    yield {'INSERT_LOG_PRECIFICACAO' : log_pool}
                    yield {'INSERT_PRODUTOGRADEPRECOGRUPO' : log_preco}
                    log_preco.clear()
                    log_pool.clear()

        yield {'INSERT_LOG_PRECIFICACAO' : log_pool}
        yield {'INSERT_PRODUTOGRADEPRECOGRUPO' : log_preco}
        log_preco.clear()
        log_pool.clear()

    def __lote_persist(self) -> None:
        """Inserindo lote de dados"""
        for data in self.__set_precificacao():
            for k,v in data.items():
                _sql = (INSERT_LOG_PRECIFICACAO if k ==
                        'INSERT_LOG_PRECIFICACAO' else INSERT_PRODUTOGRADEPRECOGRUPO)
                self.insert_many(_sql, v)

    def __del__(self):
        """del conexão"""
        self.non_chance_price.clear()
        self.cache.clear()
        self.cache.close()
