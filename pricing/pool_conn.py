"""Módulo de precificação e criação do multi-grupo preço MM."""
import atexit
import os
import time
from threading import Lock
import psycopg
from psycopg_pool import ConnectionPool
from psycopg import OperationalError

_reconnect_lock = Lock()

def config_conn(conn):
    """Eliminando prepare"""
    conn.prepare_threshold = 0

@staticmethod
def check_connection(conn):
    """Testando a conexão antes do uso"""
    if conn.info.transaction_status != psycopg.pq.TransactionStatus.IDLE:
        raise psycopg.OperationalError("Conexão não está limpa")
    with conn.cursor() as cur:
        cur.execute("SELECT 1")

def on_reconnect_failed(p: ConnectionPool):
    """Tenta reconectar no banco em caso de falha"""
    with _reconnect_lock:
        max_tentativas = 5
        intervalo_inicial = 30  # segundos
        for tentativa in range(1, max_tentativas + 1):
            try:
                with p.connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("SELECT 1")
                return
            except OperationalError:
                time.sleep(intervalo_inicial * tentativa)
        raise ConnectionError("Não foi possível reconectar ao banco após várias tentativas.")

pool = ConnectionPool(
        conninfo=os.getenv("MMDBDEV",''),
        open=False,
        min_size=1,
        max_size=10,
        max_waiting=30,
        timeout=5,
        max_lifetime=1800,
        max_idle=300,
        check=check_connection,
        reconnect_failed=on_reconnect_failed,
        configure=config_conn
    )
pool.open()
pool.wait()
atexit.register(pool.close)
