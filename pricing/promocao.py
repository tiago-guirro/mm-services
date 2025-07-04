"""Módulo de precificação e criação do multi-grupo preço MM."""
import psycopg
from pricing.utils.log import logger
from pricing.pool_conn import pool
from pricing.sql import SQL_SALES_DISABLE

def sales_disable():
    """Desabilitando promoções"""
    try:
        with pool.connection() as conn:
            conn.execute(SQL_SALES_DISABLE, prepare=False)
            conn.commit()
        # logger.info('Promoção disable success!')
    except psycopg.Error as e:
        logger.error("Promoção %s", e)
