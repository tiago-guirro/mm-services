"""Módulo de precificação e criação do multi-grupo preço MM."""
import atexit
import os
from threading import Lock
import psycopg
from psycopg import Connection
from psycopg.pq import TransactionStatus
from psycopg.errors import Error as PsycopgError
from psycopg_pool import ConnectionPool
from utils.log import logger

_reconnect_lock = Lock()

def config_conn(conn):
    """Eliminando prepare"""
    conn.prepare_threshold = None

def reset_connection(conn: psycopg.Connection) -> None:
    """Reseta uma conexão antes de devolvê-la ao pool"""
    try:
        if _reconnect_lock:
            ts = conn.info.transaction_status
            if ts in (TransactionStatus.INTRANS, TransactionStatus.INERROR):
                logger.debug("Conexão ainda em transação, aplicando rollback.")
                conn.rollback()
            if ts == TransactionStatus.UNKNOWN:
                logger.error("Conexão em estado UNKNOWN — fechando conexão.")
                conn.close()
    except PsycopgError as e:
        logger.warning("Erro ao resetar conexão, forçando fechamento: %s", e)
        conn.close()

def check_connection(conn: Connection) -> None:
    """Valida a conexão antes de entregar ao cliente. Recupera se possível."""
    try:
        if _reconnect_lock:
            ts = conn.info.transaction_status
            if ts in (TransactionStatus.INTRANS, TransactionStatus.INERROR):
                logger.warning("check: transação com erro — rollback.")
                conn.rollback()
            if ts == TransactionStatus.UNKNOWN:
                logger.error("check: conexão em estado UNKNOWN — descartando.")
                conn.close()
                # raise OperationalError("Conexão em estado desconhecido.")
            with conn.cursor() as cur:
                cur.execute("SELECT 1", prepare=False)
                cur.fetchone()

    except PsycopgError as e:
        logger.error("check: falha ao validar conexão — descartando. Erro: %s", e)
        conn.close()
        # raise OperationalError("Conexão inválida mesmo após tentativa de recuperação.") from e

def on_reconnect_failed(_pool: ConnectionPool):
    """Tentanto reconexão ao falhar"""
    logger.critical("🚨 FALHA ao tentar reconectar ao banco após timeout.")
    try:
        if _reconnect_lock:
            logger.info("Tentando reiniciar o pool manualmente...")
            _pool.close()
            _pool.open()
            logger.info("Pool reiniciado com sucesso.")
    except Exception as e:
        logger.critical("Erro ao reiniciar o pool: %s", e)

pool = ConnectionPool(
        conninfo=os.getenv("MMDBDEV",''),
        open=True,
        min_size=1,
        max_size=10,
        max_waiting=0,
        timeout=60,
        max_lifetime=3600,
        max_idle=600,
        reconnect_timeout=600,
        reset=reset_connection,
        check=check_connection,
        reconnect_failed=on_reconnect_failed,
        configure=config_conn
    )

atexit.register(pool.close)
