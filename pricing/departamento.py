"""Módulo de precificação e criação do multi-grupo preço MM."""
# -*- coding: latin-1 -*-
import json
from functools import lru_cache
import psycopg
from psycopg.rows import dict_row
from pricing.sqlcomposition import SQLComposition
from pricing.sql import ARVORE_DEPARTAMENTO, ARVORE_IDPRODUTOS

sql = SQLComposition()

@lru_cache(maxsize=None)
def arvore_departamento(cur, iddepartamento):
    """Carregando departamento"""
    cur.execute(ARVORE_DEPARTAMENTO,[iddepartamento], prepare=False)
    dados = cur.fetchall()
    if dados:
        return json.dumps(list(cur.fetchall()), sort_keys=True, indent=4, ensure_ascii=False)
    return None

def cache_departamento(pool, capture_exception, logger):
    """Carregando departamento"""
    with pool.connection() as conn:
        with conn.cursor(row_factory=dict_row) as cursor:
            departamentos = cursor.execute(ARVORE_IDPRODUTOS, prepare=False).fetchall()
            for departamento in departamentos:
                arvore = arvore_departamento(cursor, departamento.get('iddepartamento'))
                if not arvore:
                    continue
                _key = {
                    'iddepartamento' : departamento.get('iddepartamento'),
                    'arvore' : arvore
                    }
                print(_key)
                try:
                    with conn.transaction():
                        print(_key)
                        query = sql.makeinsertquery('arvore_departamento',_key,'ecode')
                        cursor.execute(query,_key, prepare=False)
                except psycopg.Error as e:
                    logger.error(f"{str(e)} departamento")
                    capture_exception(e)
