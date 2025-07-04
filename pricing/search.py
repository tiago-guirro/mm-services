"""Módulo de precificação e criação do multi-grupo preço MM."""
import psycopg
from pricing.utils.log import logger
from pricing.pool_conn import pool
from pricing.sql import PRODUTOS_TSVECTOR

def atualizacao_search():
    """Atualizacao"""
    logger.info("atualizacao_search")
    try:
        with pool.connection() as conn:
            with conn.cursor() as cursor:
                with conn.transaction():
                    cursor.execute(PRODUTOS_TSVECTOR, prepare=False)
            conn.commit()
            # logger.info("Atualização realizada com sucesso.")
    except psycopg.Error as e:
        logger.error("%s atualizacao_search", e)
