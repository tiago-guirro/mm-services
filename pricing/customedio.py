"""Módulo de precificação e criação do multi-grupo preço MM."""
from queue import Queue, Empty
import psycopg
from psycopg.rows import dict_row
from pool_conn import pool
from utils.log import log_error, log_notify
from sql import (
    SQL_LOAD_PRODUTO_FILIAL,
    SQL_GET_FILIAIS_PRECIFICAR,
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

    def _set_write(self, data):
        try:
            with pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.executemany(SQL_UPSERT_CUSTOMEDIO,data)
                conn.commit()
        except (psycopg.Error,
                psycopg.errors.DuplicatePreparedStatement,
                psycopg.errors.InvalidSqlStatementName) as e:
            log_error(f"_upsert_customedio {e}")

    def _upsert_customedio(self):
        n = 1
        gravando = []
        total = c_log.qsize()
        while not c_log.empty():
            try:
                gravando.append(c_log.get_nowait())
                c_log.task_done()
                if n % 300 == 0:
                    self._set_write(gravando)
                    log_notify(f"Gravando custo: {n} de {total}.")
                    gravando.clear()
                n += 1
            except Empty:
                break
        self._set_write(gravando)
        log_notify(f"Gravando custo: {n} de {total}.")
        gravando.clear()

    def _load_produtos_filial(self):
        for idfilial in self._filiais:
            with pool.connection() as conn:
                with conn.cursor(row_factory=dict_row) as cur:
                    cur.execute(SQL_LOAD_PRODUTO_FILIAL, {'idfilial' : idfilial}, prepare=False)
                    for c in cur:
                        if isinstance(c, dict):
                            c_log.put(c)
        self._upsert_customedio()

if __name__ == "__main__":
    CustoMedio()
