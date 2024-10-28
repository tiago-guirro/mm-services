"""Módulo de precificação e criação do multi-grupo preço MM."""
from decimal import Decimal
from diskcache import Cache
import psycopg
from psycopg.rows import dict_row
from pricing.sql import (
    SQL_LOAD_PRODUTO_FILIAL,
    SQL_GET_CUST_MEDIO,
    SQL_GET_FILIAIS_PRECIFICAR,
    SQL_GET_CUST_MEDIO_REMARCACAO,
    SQL_PERSISTENCIA_CUSTO
    )

class CustoMedio:

    """Criando custos médios para precificação"""

    __URL = '/home/ecode/Python/MM/cache'

    def __init__(self, pool, capture_exception, logger):
        """Iniciando conexão"""
        self._filiais: list = []
        self._remarcacao: list = []
        self._pool = pool
        self._capture_exception = capture_exception
        self._logger = logger
        self._logger.info("CustoMedio")
        self._cache = Cache(self.__URL)
        self._cache.clear()
        self._load_custo_medio_remarcacao()
        self._load_filiais_precificar()
        self._load_produtos_filial()

    def _setting_error(self, local, error) -> None:
        self._logger.error(f"{str(error)} {local}")
        self._capture_exception(error)

    def _load_filiais_precificar(self) -> None:
        with self._pool.connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                with conn.transaction():
                    cur.execute(SQL_GET_FILIAIS_PRECIFICAR, prepare=False)
                    for c in cur:
                        self._filiais.append(c.get('idfilialsaldo'))

    def _load_custo_medio_funcao(self, conn, params):
        try:
            conn.row_factory=dict_row
            custo_medio = conn.execute(
                SQL_GET_CUST_MEDIO,
                params,
                prepare=False).fetchone()
            if (custo_medio.get('custo_calc_unit')
                and custo_medio.get('custo_calc_unit',0)):
                return custo_medio
            raise psycopg.Error('Preço zero')
        except psycopg.Error:
            return None

    def _load_custo_medio_remarcacao(self):
        try:
            with self._pool.connection() as conn:
                with conn.cursor(row_factory=dict_row) as cur:
                    with conn.transaction():
                        cur.execute(SQL_GET_CUST_MEDIO_REMARCACAO, prepare=False)
                        self._remarcacao = cur.fetchall()
        except psycopg.Error:
            return None
        return None

    def _pesquisa_custo_medio_remarcacao(self, idfilial, idproduto, idgradex, idgradey):
        if idfilial not in (10050,10001,10083):
            return None
        for pd in self._remarcacao:
            if (pd.get('idproduto') == idproduto
                and pd.get('idgradex') == idgradex
                and pd.get('idgradey') == idgradey):
                return {
                    "custo_calc_unit" : pd.get('custo_calc_unit'),
                    "vlr_icms_st_recup_calc" : pd.get('vlr_icms_st_recup_calc'),
                    "origem_reg" : pd.get('origem_reg')
                    }
        return None

    def _gravacao_customedio(self, params):
        with self._pool.connection() as conn:
            conn.execute(SQL_PERSISTENCIA_CUSTO, params, prepare=False)

    def _load_produtos_filial(self):
        for idfilial in self._filiais:
            with self._pool.connection() as conn:
                with conn.cursor(row_factory=dict_row) as cur:
                    cur.execute(SQL_LOAD_PRODUTO_FILIAL, [idfilial], prepare=False)
                    for c in cur:
                        custo_medio = (self._load_custo_medio_funcao(conn,c)
                                        or self._pesquisa_custo_medio_remarcacao(
                                            idfilial,
                                            c.get('idproduto'),
                                            c.get('idgradex'),
                                            c.get('idgradey')))
                        if (custo_medio
                            and custo_medio.get('custo_calc_unit',Decimal(0)) > Decimal(0)
                            and custo_medio.get('custo_calc_unit') != c.get('custo_calc_unit')):
                            c.update(custo_medio)
                            self._gravacao_customedio(c)
