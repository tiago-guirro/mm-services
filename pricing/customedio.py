"""Módulo de precificação e criação do multi-grupo preço MM."""
from diskcache import Cache
import psycopg
from psycopg.rows import dict_row
from pricing.sql import (
    SQL_LOAD_PRODUTO_FILIAL,
    SQL_GET_CUST_MEDIO,
    SQL_GET_FILIAIS_PRECIFICAR,
    SQL_GET_CUST_MEDIO_REMARCACAO,
    SQL_PERSISTENCIA_CUSTO,
    SQL_UPSERT_CUSTOMEDIO
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
            if not custo_medio:
                return False
            params.update({'atualizar' : True})
            if (params.get('custo_calc_unit',0) == custo_medio.get('custo_calc_unit',0) and
                params.get('vlr_icms_st_recup_calc',0) == custo_medio.get('vlr_icms_st_recup_calc',0) and
                params.get('vlr_icms_proprio_entrada_unit',0) == custo_medio.get('vlr_icms_proprio_entrada_unit',0)):
                params.update({'atualizar' : False})
            params |= custo_medio
            return True
        except psycopg.Error as e:
            self._setting_error('_load_custo_medio_funcao', e)
            return False

    def _load_custo_medio_remarcacao(self):
        try:
            with self._pool.connection() as conn:
                with conn.cursor(row_factory=dict_row) as cur:
                    with conn.transaction():
                        cur.execute(SQL_GET_CUST_MEDIO_REMARCACAO, prepare=False)
                        self._remarcacao = cur.fetchall()
        except psycopg.Error as e:
            self._setting_error('_load_custo_medio_remarcacao', e)
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
        try:
            with self._pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.executemany(SQL_PERSISTENCIA_CUSTO, params)
        except (psycopg.errors.DuplicatePreparedStatement,
                psycopg.errors.InvalidSqlStatementName):
            self._gravacao_customedio(params)
        except psycopg.Error as e:
            self._setting_error('_gravacao_customedio', e)

    def _upsert_customedio(self, conn, params):
        try:
            return conn.execute(SQL_UPSERT_CUSTOMEDIO, params).fetchone()
        except (psycopg.Error,
                psycopg.errors.DuplicatePreparedStatement,
                psycopg.errors.InvalidSqlStatementName) as e:
            self._setting_error('_upsert_customedio', e)

    def _load_produtos_filial(self):
        for idfilial in self._filiais:
            with self._pool.connection() as conn:
                with conn.cursor(row_factory=dict_row) as cur:
                    cur.execute(SQL_LOAD_PRODUTO_FILIAL, {'idfilial' : idfilial}, prepare=False)
                    for c in cur:
                        if not c:
                            continue
                        self._upsert_customedio(conn, c)
