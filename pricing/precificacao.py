"""Módulo de precificação e criação do multi-grupo preço MM."""
import gc
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
        self._del_cache: list = []
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
                        self._del_cache.append('Frete')
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
                self._del_cache.append(_key_frete_search)
                self.cache.set(_key_frete_search,_cache)
                return _cache
        return 0

    def _base_preco_comparacao(self):
        with self.pool.connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                with conn.transaction():
                    preco = cur.execute(SQL_LOAD_PRECOS_TOTAL, prepare=False).fetchall()
        comparacao: dict = {}
        for p in preco:
            key: str = f"{p.get('idgrupopreco')}.{p.get('idproduto')}.{p.get('idgradex')}.{p.get('idgradey')}"
            comparacao.update(
                {
                    key : {
                        "precovenda" : round(p.get('precovenda'),2),
                        "margem" : p.get('margem'),
                        "frete" : p.get('frete'),
                        "icms" : p.get('icms')
                        }
                })
        return comparacao

    def _get_customedio_ajustado(self, **k):
        try:
            with self.pool.connection() as conn:
                with conn.cursor(row_factory=dict_row) as cur:
                    with conn.transaction():
                        return cur.execute(SQL_INIT_TEST, k, prepare=False).fetchall()
        except psycopg.Error as e:
            self.setting_error('get_customedio', e)
            return None

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
        self._del_cache.append(_key)
        self.cache.set(_key,_cache)
        return _cache

    def get_calc_sales_price(self, customedio: Decimal, rl: dict, idsituacaoorigem: int):
        """Coletando no cache os dados por produto"""
        if (rl.get('idgrupopreco') != 1005
            and rl.get('idfilial') != 10281
            and idsituacaoorigem in (1,2,3,8)):
            rl.update({"icms" : Decimal(4.0)})

        _desoneracao: float = ((100 - rl.get('icms')) / 100)
        rll = rl.copy()
        rll.update({"pis" : round(rl.get('pis') * _desoneracao,4)})
        rll.update({"cofins" : round(rl.get('cofins') * _desoneracao,4)})
        keys: list = ['icms','margem','adicional','frete','pis','cofins']
        _idx: float = sum(rll.get(i) for i in keys)
        del rll, _desoneracao
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
        except (psycopg.errors.DuplicatePreparedStatement, psycopg.errors.InvalidSqlStatementName):
            self.insert_many(sql, bigdata)
        except psycopg.Error as e:
            self.setting_error('insert_many', e)

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

    def _set_hash(self, line: dict):
        return '|'.join([str(x or '') for x in line.values()])

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
        base = self._base_preco_comparacao()


        

        for rul_o in rules:
            # Listando regras por ordem de importancia
            custos = self._get_customedio_ajustado(
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
                key: str = f"{rul.get('idgrupopreco')}.{custo.get('idproduto')}.{custo.get('idgradex')}.{custo.get('idgradey')}"
                if self.__no_duplicate_key(
                    idproduto=custo.get('idproduto'),
                    idgradex=custo.get('idgradex'),
                    idgradey=custo.get('idgradey'),
                    idgrupopreco=rul.get('idgrupopreco')
                    ):
                    continue

                # Verificando o formato de agrupamento por idproduto.idgradex.idgradey ou idproduto
                _customedio = custo.get('customedio_agrupado') if rul.get(
                    'agrupar_x_y', 'Não') == 'Sim' else custo.get('customedio')
                _frete = self.get_frete_search(
                    rul.get('idgrupopreco'), custo.get('classificacao'))
                if _frete > 0:
                    rul.update({'frete' : _frete})
                price: Decimal = round(Decimal(self.get_calc_sales_price(
                    _customedio, rul, custo.get('idsituacaoorigem', 0))),2)

                # Validando preço zero ou igual customedio
                if price <= _customedio or price <= 0:
                    raise ValueError('Custo abaixo do permitido')

                base_comparacao = base.get(key,{})

                # Verificando se existe o mesmo preço
                if (base_comparacao.get('precovenda') == price
                    and base_comparacao.get('icms') == Decimal(rul.get('icms'))
                    and base_comparacao.get('frete') == Decimal(rul.get('frete'))
                    and base_comparacao.get('margem') == Decimal(rul.get('margem'))
                    ):
                    continue

                base.update(
                    {
                        key: {
                            'precovenda': price,
                            'icms': Decimal(rul.get('icms')),
                            'frete': Decimal(_frete),
                            'margem': Decimal(rul.get('margem'))
                            }
                        }
                    )

                totalizador += 1

                log_pool.append(
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

                log_preco.append(
                    {
                        "idproduto": custo.get('idproduto'),
                        "idgradex": custo.get('idgradex'),
                        "idgradey": custo.get('idgradey'),
                        "precocusto": _customedio,
                        "idgrupopreco": rul.get('idgrupopreco'),
                        "precovenda": price

                    }
                )

                if totalizador % 50 == 0:
                    yield {'INSERT_LOG_PRECIFICACAO' : log_pool}
                    yield {'INSERT_PRODUTOGRADEPRECOGRUPO' : log_preco}
                    log_preco.clear()
                    log_pool.clear()

            if len(log_pool) > 0:
                yield {'INSERT_LOG_PRECIFICACAO' : log_pool}
            if len(log_preco) > 0:
                yield {'INSERT_PRODUTOGRADEPRECOGRUPO' : log_preco}
            log_preco.clear()
            log_pool.clear()
            gc.collect()

    def __lote_persist(self) -> None:
        """Inserindo lote de dados"""
        for data in self.__set_precificacao():
            for k,v in data.items():
                _sql = (INSERT_LOG_PRECIFICACAO if k ==
                        'INSERT_LOG_PRECIFICACAO' else INSERT_PRODUTOGRADEPRECOGRUPO)
                self.insert_many(_sql, v)

    def __del__(self):
        """del conexão"""
        for key in self._del_cache:
            if key in self.cache:
                del self.cache[key]
        self.non_chance_price.clear()
        self.cache.clear()
        self.cache.close()
