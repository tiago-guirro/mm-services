"""Módulo de precificação e criação do multi-grupo preço MM."""
import psycopg
from pricing.sql import SQL_SALES_DISABLE

def sales_disable(pool, logger):
    """Desabilitando promoções"""
    try:
        with pool.connection() as conn:
            conn.execute(SQL_SALES_DISABLE, prepare=False)
        logger.info('Promoção disable success!')
    except psycopg.Error as e:
        logger.error(f"Promoção {e}")
