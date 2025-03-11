# -*- coding: latin-1 -*-
"""Módulo de precificação e criação do multi-grupo preço MM."""
import psycopg
from pricing.sql import PRODUTOS_TSVECTOR

def atualizacao_search(pool, logger):
    """Atualizacao"""
    logger.info("atualizacao_search")
    try:
        with pool.connection() as conn:
            with conn.cursor() as cursor:
                with conn.transaction():
                    cursor.execute(PRODUTOS_TSVECTOR, prepare=False)
    except psycopg.Error as e:
        logger.error(f"{str(e)} atualizacao_search")
