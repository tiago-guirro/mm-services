"""Módulo de precificação e criação do multi-grupo preço MM."""
from psycopg import sql

class SQLComposition:
    """Gravação regras de negócio MM"""

    def makeupsertquery(self, table_name:str, key_update:dict, value_update:dict, key_insert:dict, schema_name='public'):
        """Criando gravação genérica para o Db"""
        query = sql.SQL("""with
                            atualizacao as (
                                update {schema}.{table} set ({value_update_keys}) = ({value_update_values}) where ({key_update_keys}) = ({key_update_values})
                                returning 1
                            ),
                            inserindo as (
                                insert into {schema}.{table} ({key_insert_keys})
                                select {key_insert_values} where not exists (select 1 from atualizacao)
                                returning 1
                            )
                            select 
                            (select count(1) from atualizacao) as atualizacao, 
                            (select count(1) from inserindo) as inserindo"""
                        ).format(
            schema=sql.Identifier(schema_name),
            table=sql.Identifier(table_name),
            key_update_keys=sql.SQL(', ').join(map(sql.Identifier, key_update.keys())),
            key_update_values=sql.SQL(', ').join(map(sql.Placeholder, key_update.keys())),
            value_update_keys=sql.SQL(', ').join(map(sql.Identifier, value_update.keys())),
            value_update_values=sql.SQL(', ').join(map(sql.Placeholder, value_update.keys())),
            key_insert_keys=sql.SQL(', ').join(map(sql.Identifier, key_insert.keys())),
            key_insert_values=sql.SQL(', ').join(map(sql.Placeholder, key_insert.keys()))
            )
        return query

    def makeinsertquery(self, table_name, fields, schema_name='public'):
        """Criando gravação genérica para o Db"""
        query = sql.SQL("INSERT INTO {schema}.{table} ({pkey}) VALUES ({values})").format(
            schema=sql.Identifier(schema_name),
            table=sql.Identifier(table_name),
            pkey=sql.SQL(',').join(list(sql.Identifier(x) for x in fields)),
            values=sql.SQL(', ').join(map(sql.Placeholder, fields)))
        return query

    def makedelquery(self, table_name, pkey, schema_name='public'):
        """Criando gravação genérica para o Db"""
        query = sql.SQL("delete from {schema}.{table} where {pkey} = {pkey_value}").format(
            schema=sql.Identifier(schema_name),
            table=sql.Identifier(table_name),
            pkey=sql.Identifier(pkey),
            pkey_value=sql.Placeholder(pkey)
        )
        return query

    def makeupdatequery(self, table_name, pkey, fields, schema_name='public'):
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
