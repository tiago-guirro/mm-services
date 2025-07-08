"""Módulo de precificação e criação do multi-grupo preço MM."""
from queue import Queue, Empty
import psycopg
from psycopg.rows import dict_row
from pricing.pool_conn import pool
from pricing.utils.log import log_error, log_notify
from pricing.sql import (
    SQL_LOAD_PRODUTO_FILIAL,
    SQL_GET_CUST_MEDIO,
    SQL_GET_FILIAIS_PRECIFICAR,
    SQL_GET_CUST_MEDIO_REMARCACAO,
    SQL_UPSERT_CUSTOMEDIO
    )

c_log = Queue()
class CustoMedio:

    """Criando custos médios para precificação"""
    def __init__(self):
        """Iniciando conexão"""
        self._filiais: list = []
        self._remarcacao: list = []
        self._load_filiais_precificar()
        self._load_produtos_filial()

    def _load_filiais_precificar(self) -> None:
        with pool.connection() as conn:
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
            validacao = [
                params.get('custo_calc_unit',0) == custo_medio.get('custo_calc_unit',0),
                params.get('vlr_icms_st_recup_calc',0) == custo_medio.get('vlr_icms_st_recup_calc',0),
                params.get('vlr_icms_proprio_entrada_unit',0) == custo_medio.get('vlr_icms_proprio_entrada_unit',0)
            ]
            if all(validacao):
                params.update({'atualizar' : False})
            params |= custo_medio
            return True
        except psycopg.Error as e:
            log_error(f"_load_custo_medio_funcao {e}")
            return False

    def _load_custo_medio_remarcacao(self):
        try:
            with pool.connection() as conn:
                with conn.cursor(row_factory=dict_row) as cur:
                    cur.execute(SQL_GET_CUST_MEDIO_REMARCACAO, prepare=False)
                    self._remarcacao = cur.fetchall()
                conn.commit()
            return None
        except psycopg.Error as e:
            log_error(f"_load_custo_medio_remarcacao {e}")
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

    def _upsert_customedio(self):
        try:
            with pool.connection() as conn:
                with conn.cursor() as cur:
                    n = 1
                    while not c_log.empty():
                        try:
                            values = c_log.get_nowait()
                            cur.execute(SQL_UPSERT_CUSTOMEDIO,
                                        values,
                                        prepare=False)
                            c_log.task_done()
                            if n % 200 == 0:
                                log_notify(f"Persist {c_log.qsize()}")
                                conn.commit()
                            n += 1
                        except Empty:
                            break
                conn.commit()
        except (psycopg.Error,
                psycopg.errors.DuplicatePreparedStatement,
                psycopg.errors.InvalidSqlStatementName) as e:
            log_error(f"_upsert_customedio {e}")

    def _load_produtos_filial(self):
        for idfilial in self._filiais:
            with pool.connection() as conn:
                with conn.cursor(row_factory=dict_row) as cur:
                    cur.execute(SQL_LOAD_PRODUTO_FILIAL, {'idfilial' : idfilial}, prepare=False)
                    for c in cur:
                        if isinstance(c, dict):
                            c_log.put(c)
        self._upsert_customedio()
