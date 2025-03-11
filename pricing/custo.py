"""Módulo de precificação e criação do multi-grupo preço MM."""
import psycopg
from pricing.sql import SQL_CACHE_CUSTO

def atualizacao_custo(pool, logger):
    """atualizacao_search"""
    logger.info("atualizacao_custo")

    try:
        with pool.connection() as conn:
            conn.execute(SQL_CACHE_CUSTO)
            conn.commit()
    except psycopg.Error as e:
        conn.rollback()
        logger.error(f"{str(e)} atualizacao_custo")
