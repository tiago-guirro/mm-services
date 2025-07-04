"""Módulo de precificação e criação do multi-grupo preço MM."""
from queue import Queue, Empty
from decimal import Decimal
import psycopg
from psycopg.rows import dict_row
from psycopg_pool import PoolTimeout
from pricing.utils.log import logger
from pricing.pool_conn import pool
from pricing.utils.cache import cache
from pricing.utils.calculos import round_salles, round_up
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
                with conn.transaction():
                    with conn.cursor(row_factory=dict_row) as cur:
                        cur.execute(SQL_LOAD_REGRA, prepare=False)
                        for c in cur:
                            regra.append(c)
        except psycopg.Error as e:
            logger.error('get_customedio %s', e)

    def get_preco_comparacao(self):
        """Retornando dados comparação"""
        try:
            with pool.connection() as conn:
                with conn.transaction():
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
                                    p.get('icms')]
                            key = '_'.join(str(c) if c is not None else '' for c in campos)
                            preco_comparacao.add(key)
        except psycopg.Error as e:
            logger.error('get_customedio %s', e)

    def get_customedio_ajustado(self, **k) -> list:
        """Custo médio ajustado"""
        @cache.memoize(expire=60*60)
        def get_data(**kk):
            try:
                with pool.connection() as conn:
                    with conn.cursor(row_factory=dict_row) as cur:
                        with conn.transaction():
                            cur.execute(SQL_INIT_TEST,kk,prepare=False)
                            return cur.fetchall()
            except psycopg.Error as e:
                logger.error('get_customedio_ajustado %s %s', e, k)
                return []
        return get_data(**k)

    def get_frete(self):
        """Load frete de toda rede"""
        try:
            with pool.connection() as conn:
                with conn.cursor(row_factory=dict_row) as cur:
                    with conn.transaction():
                        cur.execute(SQL_FRETE_TOTAL, prepare=False)
                        for c in cur:
                            regra.append(c)
        except psycopg.Error as e:
            logger.error('get_frete %s', e)

    def insert_many(self):
        """Inserindo lote de dados"""
        try:
            # logger.info("Iniciando Gravação: %s", w_precos.qsize())
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
                                # logger.info("Gravando: %s %s",controle,w_precos.qsize())
                                safe.clear()
                                controle = 0
                        except Empty:
                            break
                conn.commit()
                for _ in range(0,controle):
                    w_precos.task_done()
                    w_log.task_done()
                    # logger.info("Gravando: %s %s",controle,w_precos.qsize())
                safe.clear()
        except (PoolTimeout,
                psycopg.errors.DuplicatePreparedStatement,
                psycopg.Error) as e:
            conn.rollback()
            logger.error("Error: %s.", e)
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
        self.ops.get_preco_comparacao()
        self.ops.get_frete()
        # logger.info("Precificacao")
        self._set_precificacao()

    def setting_error(self, local, error) -> None:
        """setting_error"""
        logger.error("%s %s", str(error), local)

    def get_frete_search(self, idgrupopreco, classificacao):
        """Coletando no cache os dados por produto"""
        @cache.memoize(expire=60*60*24)
        def get_data(_idgrupopreco, _classificacao):
            for f in frete:
                if not f.get('idgrupopreco') == _idgrupopreco:
                    continue
                if _classificacao.startswith(f.get('classificacao'),0):
                    _cache = f.get('frete',0)
                    return _cache
            return 0
        return get_data(idgrupopreco, classificacao)

    def get_calc_sales_price(self, customedio: Decimal, rl: dict, idsituacaoorigem: int):
        """Coletando no cache os dados por produto"""
        if (rl.get('idgrupopreco') != 1005
            and rl.get('idfilial') != 10281
            and idsituacaoorigem in (1,2,3,8)):
            rl.update({"icms" : Decimal(4.0)})
        _desoneracao: float = ((100 - rl.get('icms',0)) / Decimal(100))
        rll = rl.copy()
        rll.update({"pis" : round_up(rl.get('pis',0) * _desoneracao)})
        rll.update({"cofins" : round_up(rl.get('cofins',0) * _desoneracao)})
        _idx: Decimal = rll.get('icms',0)
        _idx += rll.get('margem',0)
        _idx += rll.get('adicional',0)
        _idx += rll.get('frete',0)
        _idx += rll.get('pis',0)
        _idx += rll.get('cofins',0)
        del rll, _desoneracao
        return round_salles(customedio / (Decimal(1) - (Decimal(_idx) / Decimal(100))))

    def insert_many(self, sql, bigdata) -> None:
        """Inserindo lote de dados"""
        if not bigdata:
            return
        try:
            with pool.connection() as conn:
                conn.autocommit = False
                conn.prepare_threshold=1000
                conn.commit()
                with conn.cursor() as cur:
                    cur.executemany(sql, bigdata)
                    conn.commit()
                    # logger.info("insert_many %s", str(len(bigdata)))
        except (psycopg.errors.DuplicatePreparedStatement, psycopg.errors.InvalidSqlStatementName):
            conn.rollback()
            self.insert_many(sql, bigdata)
        except psycopg.Error as e:
            conn.rollback()
            self.setting_error('insert_many', e)

    def _set_precificacao(self):
        """Criando precificação"""
        totalizador: int = 1
        for rul_o in regra:
            # Listando regras por ordem de importancia
            # if not rul_o.get('idfilialsaldo'):
            #     continue
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
                _customedio = custo.get('customedio_agrupado') if rul.get(
                    'agrupar_x_y', 'Não') == 'Sim' else custo.get('customedio')
                _frete = self.get_frete_search(rul.get('idgrupopreco',0),
                                               custo.get('classificacao','0'))
                if _frete > 0:
                    rul.update({'frete' : _frete})
                price: Decimal = self.get_calc_sales_price(_customedio,
                                                           rul,
                                                           custo.get('idsituacaoorigem',0))
                # Validando preço zero ou igual customedio
                if price <= _customedio or price <= 0:
                    raise ValueError('Custo abaixo do permitido')
                key += f"_{price}_"
                key += f"{Decimal(rul.get('margem',0))}_"
                key += f"{Decimal(rul.get('frete',0))}_"
                key += f"{Decimal(rul.get('icms',0))}"
                if key in preco_comparacao:
                    continue
                preco_comparacao.add(key)
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
                # logger.info('Adicionado %s.', key, )
            # logger.info('Total %s.', w_precos.qsize())
        if w_precos.qsize() > 0:
            self.ops.insert_many()

    def __del__(self):
        """del conexão"""
        cache.evict(tag='Atacado')
        regra.clear()
        frete.clear()
        preco_comparacao.clear()
        no_duplicate_key.clear()
