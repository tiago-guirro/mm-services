"""Gestao precificacao ecommerce"""
from typing import Any
import hashlib
import time
from collections import defaultdict
from queue import Queue, Empty
from decimal import Decimal
from concurrent.futures import ThreadPoolExecutor, wait
from threading import Lock
from itertools import islice
import inspect
import psycopg
from psycopg_pool import PoolTimeout
from psycopg.rows import dict_row
from pricing.utils.cache import cache
from pricing.query.ecommerce_produtos import SQL as sql_produtos
from pricing.query.ecommerce_listando_processo import SQL as sql_processos
from pricing.query.ecommerce_imposto import SQL as sql_impostos
from pricing.query.ecommerce_regra_precificacao import SQL as sql_regra
from pricing.sql import (INSERT_LOG_PRECIFICACAO,
                         INSERT_PRODUTOGRADEPRECOGRUPO,
                         SQL_LOAD_PRECOS_TOTAL)
from pricing.utils.params import VINCULO, PROPORCAO_GERAL
from pricing.utils.log import logger
from pricing.pool_conn import pool
from pricing.utils.calculos import round_salles, round_up, round_two


def gerar_hash(chave: str) -> str:
    """Gerando hash"""
    return hashlib.sha256(chave.encode('utf-8')).hexdigest()

# cache.evict(tag='Ecommerce')
w_precos = Queue()
w_log = Queue()
dual = set()
preco_comparacao = set()
lock = Lock()

class Operacoes:
    """Pegando dados banco"""
    def __init__(self) -> None:
        self._expires: int | None = 0

    def get_produtos(self, **argv):
        """Dados dos produtos"""
        self._expires = 60 * 60
        return self._execute_data(sql_produtos, **argv)

    def get_regra(self):
        """Regra da precificacao"""
        self._expires = 60 * 60 * 24
        return self._execute_data(sql_regra)

    def get_impostos(self, *, uf_origem, idproduto) -> list:
        """Retornando base imposto"""
        contents: dict = {}
        contents.update({'uf_origem': uf_origem})
        contents.update({'idproduto': idproduto})
        self._expires = None
        return self._execute_data(sql_impostos, **contents)

    def get_estrutura_uf_venda(self):
        """Retornando relação uf vendas"""
        contents: dict = {}
        self._expires = 60 * 60 * 24 * 7
        return self._execute_data(sql_processos, **contents)

    def get_preco_comparacao(self):
        """Retornando dados comparação"""
        try:
            if len(preco_comparacao) > 0:
                return
            contents: dict = {}
            with pool.connection() as conn:
                with conn.cursor(row_factory=dict_row) as cur:
                    cur.execute(SQL_LOAD_PRECOS_TOTAL, contents)
                    for p in cur:
                        campos = [p.get('idgrupopreco'),
                                p.get('idproduto'),
                                p.get('idgradex'),
                                p.get('idgradey'),
                                p.get('precovenda'),
                                p.get('margem'),
                                p.get('icms')]
                        key = '_'.join(str(c) if c is not None else '' for c in campos)
                        preco_comparacao.add(gerar_hash(key))
        except psycopg.Error as e:
            logger.error('get_customedio %s', e)
            time.sleep(5)
            self.get_preco_comparacao()

    def insert_many(self):
        """Inserindo lote de dados"""
        try:
            # logger.info("Iniciando Gravação: %s", w_precos.qsize())
            with pool.connection() as conn:
                with conn.cursor() as cur:
                    controle = 0
                    safe = []
                    while not w_precos.empty():
                        try:
                            preco = w_precos.get_nowait()
                            log = w_log.get_nowait()
                            safe.append([preco, log])
                            cur.execute(INSERT_PRODUTOGRADEPRECOGRUPO,preco,prepare=False)
                            cur.execute(INSERT_LOG_PRECIFICACAO,log,prepare=False)
                            controle += 1
                            if controle % 150 == 0:
                                conn.commit()
                                for _ in range(0,controle):
                                    w_precos.task_done()
                                    w_log.task_done()
                                # logger.info("Gravando: %s %s",controle,w_precos.qsize())
                                safe.clear()
                                controle = 0
                        except Empty:
                            break
                conn.commit()
                for _ in range(0,controle):
                    w_precos.task_done()
                    w_log.task_done()
                    # logger.info("Gravando: %s %s",controle,w_precos.qsize())
                safe.clear()
        except (PoolTimeout,
                psycopg.errors.DuplicatePreparedStatement,
                psycopg.Error) as e:
            conn.rollback()
            logger.error("Error: %s.", e)
            for s in safe:
                w_precos.put(s[0])
                w_log.put(s[1])
            safe.clear()

    def _execute_data(self, sql, **contents) -> list:
        """Retornando relação uf vendas"""
        try:
            caller = inspect.stack()[1].function
            key = caller + "_"+"_".join(str(x) for x in contents.values())
            val: Any = cache.get(key)
            if key in cache and isinstance(val, list):
                return val
            with lock:
                val: Any = cache.get(key)
                if key in cache and isinstance(val, list):
                    return val
                # logger.info("Carregando dados: %s", key)
                with pool.connection() as conn:
                    with conn.cursor(row_factory=dict_row) as cur:
                        cur.execute(sql, contents)
                        to_cache_ = cur.fetchall()
                        cache.set(key, to_cache_, tag='Ecommerce',expire=self._expires)
                    conn.commit()
                return to_cache_
        except (PoolTimeout, psycopg.errors.DuplicatePreparedStatement) as e:
            logger.error("Erro, aguardando 60s: %s.", e)
            time.sleep(60)
            return self._execute_data(sql, **contents)
        except psycopg.Error as e:
            logger.error("psycopg.Error: %s.", e)
            return []

class EcommerceUnique:
    """Gestao precificacao ecommerce"""
    def __init__(self) -> None:
        self.ops = Operacoes()
        self._historico_venda = self._estrutura_regra()
        # logger.info('Histórico carregado.')
        self._regra = self.ops.get_regra()
        # logger.info('Regra carregada.')
        self.ops.get_preco_comparacao()
        # logger.info('Comparação carregada.')

    def modelo_gravacao_preco(self, fila, **k) -> None:
        """Retornando model"""
        if fila == 'preco':
            w_precos.put(k)
            return
        w_log.put(k)
        return

    def _estrutura_regra(self):
        base_impostos = defaultdict(list)
        for r in self.ops.get_estrutura_uf_venda():
            base_impostos[r.get("descricao_processo")].append({
                "uf": r.get("uf"),
                "pcto": r.get("pcto")
            })
        return dict(base_impostos)

    def montagem_regra(self, idgrupopreco):
        """Montando regra de preços"""
        for regra, produtos in self._produto_listagem(idgrupopreco):
            key_historico = VINCULO.get(regra.get('idgrupopreco'), 'GERAL')
            log = f"Grupo: {regra.get('idgrupopreco')} | "
            log += f"Classificacao: {regra.get('classificacao')} | "
            log += f"Seller: {key_historico} | "
            log += f"Total produtos: {len(produtos)}"
            # logger.info(log)
            historico = self._historico_venda.get(key_historico, PROPORCAO_GERAL)
            for produto in produtos:
                key = f"{regra.get('idgrupopreco')}_"
                key += f"{produto.get('idproduto')}_"
                key += f"{produto.get('idgradex')}_"
                key += str(produto.get('idgradey'))
                if key in dual:
                    print('chega dual')
                    continue
                dual.add(key)
                hist = self._pos_init(produto.get('idproduto'), historico)
                precos = []
                icms = []
                for h in hist:
                    idx = Decimal(100)
                    idx -= h.get("icms_destino") or h.get("icms_origem")
                    idx -= h.get("pis_cofins")
                    idx -= regra.get("margem")
                    idx -= regra.get("adicional")
                    idx /= 100
                    custo_medio = produto.get("customedio")
                    preco = custo_medio / idx
                    preco_absoluto = round_up(preco * h.get("pcto"))
                    icms.append((h.get("icms_destino") or h.get(
                        "icms_origem")) * h.get("pcto"))
                    precos.append(preco_absoluto)
                preco_final = round_salles(sum(precos) / Decimal(100))
                icms_final = round_two(sum(icms) / Decimal(100))
                key += f"_{preco_final}_"
                key += f"{regra.get("margem")}_"
                key += f"{icms_final}"
                key_ = gerar_hash(key)
                if key_ in preco_comparacao:
                    continue
                self.modelo_gravacao_preco(
                    'log',
                    idfilial=regra.get('idfilial'),
                    idfilialsaldo=regra.get('idfilialsaldo'),
                    idgrupopreco=regra.get('idgrupopreco'),
                    idproduto=produto.get('idproduto'),
                    idgradex=produto.get('idgradex'),
                    idgradey=produto.get('idgradey'),
                    margem=regra.get('margem'),
                    icms=icms_final,
                    pis=regra.get('pis'),
                    cofins=regra.get('cofins'),
                    frete=0,
                    adicional=regra.get('adicional'),
                    customedio=produto.get('customedio'),
                    precovenda=preco_final,
                    regra=key_historico
                )
                self.modelo_gravacao_preco(
                    'preco',
                    idproduto=produto.get('idproduto'),
                    idgradex=produto.get('idgradex'),
                    idgradey=produto.get('idgradey'),
                    idgrupopreco=regra.get('idgrupopreco'),
                    precocusto=produto.get('customedio'),
                    precovenda=preco_final
                )
        if w_precos.qsize() > 0:
            self.ops.insert_many()

    def _produto_listagem(self, idgrupopreco):
        for rule in self._regra:
            if rule.get('idgrupopreco') != idgrupopreco:
                continue
            produtos = self.ops.get_produtos(**{
                "idfilial": rule.get('idfilialsaldo'),
                "ncm": rule.get('ncm'),
                "classificacao": rule.get('classificacao'),
                "origem": rule.get('origem'),
                "idmarca": rule.get('idmarca'),
                "idproduto": rule.get('idproduto'),
                "idgradex": rule.get('idgradex'),
                "idgradey": rule.get('idgradey')
            })
            if len(produtos) == 0:
                continue
            yield rule, produtos

    def _pos_init(self, idproduto, regra):
        ufs_destino = [x['uf'] for x in regra]
        impostos = self.ops.get_impostos(
            uf_origem='PR',
            idproduto=idproduto)
        impostos_ = []
        for i in impostos:
            if i.get('uf_destino') in ufs_destino:
                impostos_.append(i)
        del impostos, ufs_destino
        base_impostos = []
        for i, r in zip(impostos_, regra):
            del i['uf_origem'], i['uf_destino']
            i.update({"pcto": r.get("pcto")})
            i.update({"uf": r.get("uf")})
            base_impostos.append(i)
        return base_impostos

def execucao_multi():
    """Executa montagem_regra em grupos de 4, com pool reaproveitado"""
    lista_ids = list(x for x in VINCULO)
    lista_ids.sort()
    def chunker(iterable, tamanho):
        it = iter(iterable)
        while True:
            grupo = list(islice(it, tamanho))
            if not grupo:
                break
            yield grupo
    e = EcommerceUnique()
    with ThreadPoolExecutor(max_workers=4) as executor:
        for grupo in chunker(lista_ids, 4):
            # logger.info("🚀 Iniciando grupo: %s", grupo)
            futures = [executor.submit(e.montagem_regra, id_) for id_ in grupo]
            wait(futures)
            # logger.info("✅ Grupo %s finalizado", grupo)
        preco_comparacao.clear()
        dual.clear()
