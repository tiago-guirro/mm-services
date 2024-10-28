"""Módulo de precificação e criação do multi-grupo preço MM."""
import os
from psycopg_pool import ConnectionPool

pool = ConnectionPool(
        conninfo=os.getenv("MMDBDEV"),
        min_size=1,             # Número mínimo de conexões que o pool irá manter
        max_size=10,            # Número máximo de conexões que o pool pode abrir
        max_waiting=5,          # Número máximo de requisições esperando por uma conexão
        timeout=5.0,            # Tempo máximo para esperar por uma conexão disponível
        max_lifetime=1800.0,    # Tempo máximo de vida de uma conexão em segundos
        max_idle=300.0,         # Tempo máximo de uma conexão ociosa antes de ser fechada
        check=ConnectionPool.check_connection,
        reconnect_failed=True
    )
