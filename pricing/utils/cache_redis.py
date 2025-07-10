"""Implementando redis"""
import os
from typing import Any
from decimal import Decimal
import json
import redis

def json_encoder(obj):
    """Codificando Decimal para entrada json"""
    if isinstance(obj, Decimal):
        return float(obj)  # ou str(obj) se preferir
    raise TypeError(f"Tipo {type(obj)} não serializável")

class RedisClient:

    """Centralizando acesso ao redis"""

    def __init__(self):
        self.client = redis.Redis(
            host=os.getenv('REDIS_HOST',''),
            port=int(os.getenv('REDIS_PORT','')),
            password=os.getenv('REDIS_PASSWORD',''),
            decode_responses=True)

    def exists(self, key: str) -> bool:
        """Verifica se a chave existe no Redis."""
        return self.client.exists(key) == 1

    def set(self, key: str, value: Any, ex: int | None = None) -> Any:
        """Define um valor com tempo opcional de expiração."""
        if isinstance(value, (dict, list, tuple)):
            value = json.dumps(value, default=json_encoder)
        return self.client.set(name=key, value=value, ex=ex)

    def get(self, key: str) -> Any | None:
        """Retorna o valor da chave (ou None)."""
        if self.client.exists(key) == 0:
            return None
        value: Any = self.client.get(key)
        if value is None:
            return None
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value

    def delete(self, key: str) -> Any | None:
        """Remove a chave e retorna quantas foram removidas."""
        return self.client.delete(key)

    def incr(self, key: str) -> Any | None:
        """Incrementa o valor inteiro da chave."""
        return self.client.incr(key)

    def expire(self, key: str, seconds: int) -> Any | None:
        """Define o tempo de expiração de uma chave."""
        return self.client.expire(key, seconds)

cache = RedisClient()
