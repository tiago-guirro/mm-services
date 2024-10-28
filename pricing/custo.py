"""Módulo de precificação e criação do multi-grupo preço MM."""
import psycopg
from pricing.sql import SQL_CACHE_CUSTO

def atualizacao_custo(pool,  capture_exception, logger):
    """atualizacao_search"""
    logger.info("atualizacao_custo")
    try:
        with pool.connection() as conn:
            with conn.cursor() as cursor:
                with conn.transaction():
                    cursor.execute(SQL_CACHE_CUSTO, prepare=False)
    except psycopg.Error as e:
        logger.error(f"{str(e)} atualizacao_custo")
        capture_exception(e)

