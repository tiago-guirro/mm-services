"""Gestao precificacao ecommerce"""
from typing import Any
import hashlib
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
from pricing.utils.cache_redis import cache as cache_redis
from pricing.query.ecommerce_produtos import SQL as sql_produtos
from pricing.query.ecommerce_listando_processo import SQL as sql_processos
from pricing.query.ecommerce_imposto import SQL as sql_impostos
from pricing.query.ecommerce_regra_precificacao import SQL as sql_regra
from pricing.sql import (INSERT_LOG_PRECIFICACAO,
                         INSERT_PRODUTOGRADEPRECOGRUPO,
                         SQL_LOAD_PRECOS_TOTAL)
from pricing.utils.params import VINCULO, PROPORCAO_GERAL
from pricing.utils.log import log_error, log_notify
from pricing.pool_conn import pool
from pricing.utils.calculos import round_salles, round_up, round_two

def gerar_hash(chave: str) -> str:
    """Gerando hash"""
    return hashlib.sha256(chave.encode('utf-8')).hexdigest()

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
        expires_ = 60 * 60
        return self._execute_data(sql_produtos, expires_, **argv)

    def get_regra(self):
        """Regra da precificacao"""
        expires_ = 60 * 60 * 24
        return self._execute_data(sql_regra, expires_)

    def get_impostos(self, *, uf_origem, idproduto) -> list:
        """Retornando base imposto"""
        contents: dict = {}
        contents.update({'uf_origem': uf_origem})
        contents.update({'idproduto': idproduto})
        expires_ = None
        return self._execute_data(sql_impostos, expires_, **contents)

    def get_estrutura_uf_venda(self):
        """Retornando relação uf vendas"""
        contents: dict = {}
        expires_ = 60 * 60 * 24 * 7
        return self._execute_data(sql_processos, expires_, **contents)

    def get_preco_comparacao(self):
        """Retornando dados comparação"""
        if len(preco_comparacao) > 0:
            return
        contents: dict = {}
        try:
            with lock:
                if len(preco_comparacao) > 0:
                    return
            with pool.connection() as conn:
                with conn.cursor(row_factory=dict_row) as cur:
                    cur.execute(SQL_LOAD_PRECOS_TOTAL, contents, prepare=False)
                    for p in cur:
                        campos = [p.get('idgrupopreco'),
                                p.get('idproduto'),
                                p.get('idgradex'),
                                p.get('idgradey'),
                                Decimal(p.get('precovenda',0)),
                                Decimal(p.get('margem',0)),
                                Decimal(p.get('icms',0))]
                        key = '_'.join(str(c) if c is not None else '' for c in campos)
                        preco_comparacao.add(gerar_hash(key))
        except psycopg.Error as e:
            log_error(f"get_customedio {e}")

    def insert_many(self):
        """Inserindo lote de dados"""
        try:
            log_notify(f"Iniciando Gravação: {w_precos.qsize()}")
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
                                log_notify(f"Gravando: {controle} {w_precos.qsize()}")
                                safe.clear()
                                controle = 0
                        except Empty:
                            break
                conn.commit()
                for _ in range(0,controle):
                    w_precos.task_done()
                    w_log.task_done()
                    log_notify(f"Gravando: {controle} {w_precos.qsize()}")
                safe.clear()
        except (PoolTimeout,
                psycopg.errors.DuplicatePreparedStatement,
                psycopg.Error) as e:
            log_error(f"Error {e}")
            for s in safe:
                w_precos.put(s[0])
                w_log.put(s[1])
            safe.clear()

    def _execute_data(self, sql, ex=None, **contents) -> Any:
        """Retornando relação uf vendas"""
        caller = inspect.stack()[1].function
        key = caller + ":"+":".join(str(x) for x in contents.values())
        if cache_redis.exists(key):
            return cache_redis.get(key)
        try:
            with lock:
                if cache_redis.exists(key):
                    return cache_redis.get(key)
            log_notify(f"Carregando dados: {key}")
            with pool.connection() as conn:
                with conn.cursor(row_factory=dict_row) as cur:
                    cur.execute(sql, contents, prepare=False)
                    to_cache_ = cur.fetchall()
                    cache_redis.set(key, to_cache_, ex=ex)
                conn.commit()
                return to_cache_
        except (PoolTimeout, psycopg.errors.DuplicatePreparedStatement) as e:
            log_error(f"Erro, PoolTimeout ou DuplicatePreparedStatement: {e}")
            return []
        except psycopg.Error as e:
            log_error(f"psycopg.Error: {e}")
            return []

class EcommerceUnique:
    """Gestao precificacao ecommerce"""
    def __init__(self) -> None:
        self.ops = Operacoes()
        self._historico_venda = self._estrutura_regra()
        log_notify('Histórico carregado.')
        self._regra = self.ops.get_regra()
        log_notify(f"Regra carregada ({len(self._regra)}).")
        self.ops.get_preco_comparacao()
        log_notify('Comparação carregada.')

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
            historico = self._historico_venda.get(key_historico, PROPORCAO_GERAL)

            for produto in produtos:

                key = f"{regra.get('idgrupopreco')}_"
                key += f"{produto.get('idproduto')}_"
                key += f"{produto.get('idgradex')}_"
                key += str(produto.get('idgradey'))

                if key in dual:
                    continue

                dual.add(key)
                hist = self._pos_init(produto.get('idproduto'), historico)
                precos = []
                icms = []

                for h in hist:
                    icms_ = Decimal(h.get("icms_destino",0)) or Decimal(h.get("icms_origem",0))
                    idx = Decimal(100)
                    idx -= icms_
                    idx -= Decimal(h.get("pis_cofins",0))
                    idx -= Decimal(regra.get("margem",0))
                    idx -= Decimal(regra.get("adicional",0))
                    idx /= Decimal(100)
                    custo_medio = Decimal(produto.get("customedio",0))
                    preco = custo_medio / idx
                    preco_absoluto = round_up(preco * Decimal(h.get("pcto",0)))
                    icms.append(icms_ * Decimal(h.get("pcto",0)))
                    precos.append(preco_absoluto)

                preco_final = round_salles(sum(precos) / Decimal(100))
                icms_final = round_two(sum(icms) / Decimal(100))

                key += f"_{preco_final}_"
                key += f"{Decimal(regra.get("margem",0))}_"
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

            log_notify(f"--- Fim regra ({w_precos.qsize()}) ---")
        if w_precos.qsize() > 0:
            self.ops.insert_many()

    def _produto_listagem(self, idgrupopreco):
        for rule in self._regra:
            try:
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
                if not produtos:
                    raise ValueError('_produto_listagem: Vazio:')
                yield rule, produtos
            except (ValueError, KeyError) as e:
                log_notify(e)
                continue
            except Exception as e: # pylint: disable=W0703
                log_notify(f'_produto_listagem: erro inesperado: {e}')
                continue

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
            log_notify(f"🚀 Iniciando grupo: {grupo}")
            futures = [executor.submit(e.montagem_regra, id_) for id_ in grupo]
            wait(futures)
            log_notify(f"✅ Grupo {grupo} finalizado")
        preco_comparacao.clear()
        dual.clear()
