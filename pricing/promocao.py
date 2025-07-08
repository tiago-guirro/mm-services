"""Módulo de precificação e criação do multi-grupo preço MM."""
import psycopg
from pricing.utils.log import log_error
from pricing.pool_conn import pool
from pricing.sql import SQL_SALES_DISABLE

def sales_disable():
    """Desabilitando promoções"""
    try:
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(SQL_SALES_DISABLE, prepare=False)
            conn.commit()
    except psycopg.Error as e:
        conn.rollback()
        log_error(f"Promoção {e}")
