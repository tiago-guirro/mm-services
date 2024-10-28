
"""Módulo de precificação e criação do multi-grupo preço MM."""
from psycopg import sql

class Gravacao:
    """Gravação regras de negócio MM"""
    def insert(self, table_name, fields, schema_name='public'):
        """Criando gravação genérica para o Db"""
        query = sql.SQL("INSERT INTO {schema}.{table} ({pkey}) VALUES ({values})").format(
            schema=sql.Identifier(schema_name),
            table=sql.Identifier(table_name),
            pkey=sql.SQL(',').join(list(sql.Identifier(x) for x in fields)),
            values=sql.SQL(', ').join(map(sql.Placeholder, fields)))
        return query

    def delete(self, table_name, pkey, schema_name='public'):
        """Criando gravação genérica para o Db"""
        query = sql.SQL("delete from {schema}.{table} where {pkey} = {pkey_value}").format(
            schema=sql.Identifier(schema_name),
            table=sql.Identifier(table_name),
            pkey=sql.Identifier(pkey),
            pkey_value=sql.Placeholder(pkey)
        )
        return query

    def update(self, table_name, pkey, fields, schema_name='public'):
        """Criando gravação genérica para o Db"""
        query = sql.SQL("update {schema}.{table} set ({fields}) = ({values}) where {pkey} = {pkey_value}").format(  # pylint: disable=line-too-long
            schema=sql.Identifier(schema_name),
            table=sql.Identifier(table_name),
            pkey=sql.Identifier(pkey),
            pkey_value=sql.Placeholder(pkey),
            fields=sql.SQL(',').join(map(sql.Identifier, fields)),
            values=sql.SQL(',').join(map(sql.Placeholder, fields))
        )
        return query
        
