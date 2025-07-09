"""Módulo de precificação e criação do multi-grupo preço MM."""

import sys
import os
from pytz import timezone
from apscheduler.schedulers.blocking import BlockingScheduler
from pricing.utils.log import logger
from pricing.pool_conn import pool
from pricing.precificacao import Precificacao
from pricing.search import atualizacao_search
from pricing.customedio import CustoMedio
from pricing.promocao import sales_disable
from pricing.precificacao_ecommerce import execucao_multi
from pricing.utils.cache import cache

# Ao reiniciar o processo (como em modo de desenvolvimento),
# limpe explicitamente os caches relacionados à base fiscal
# para evitar reutilização de dados obsoletos.
if not os.getenv('LOG'):
    cache.evict(tag='Atacado')   # Remove dados em cache para o canal Atacado
    cache.evict(tag='Ecommerce') # Remove dados em cache para o canal Ecommerce

tmzn = timezone('America/Sao_Paulo')
scheduler = BlockingScheduler()

scheduler.add_job(sales_disable,
                  'cron',
                  hour='7-20',
                  minute='*',
                  timezone=tmzn,
                  max_instances=1,
                  id="Desabilitar_Promocao")
scheduler.add_job(CustoMedio,
                  'cron',
                  hour='7-19',
                  minute="*/10",
                  timezone=tmzn,
                  max_instances=1,
                  id="Atualizar_Custo_Medio")
scheduler.add_job(Precificacao,
                  'cron',
                  day_of_week='mon-sat',
                  hour='7-19',
                  minute="*/15",
                  timezone=tmzn,
                  max_instances=1,
                  id="Precificacao_Atacado")
scheduler.add_job(atualizacao_search,
                  'cron',
                  day_of_week='mon-fri',
                  hour=20,
                  minute=0,
                  timezone=tmzn,
                  max_instances=1,
                  id="Atualizacao_Pesquisa")
scheduler.add_job(execucao_multi,
                  'cron',
                  hour='7-19',
                  minute="*/30",
                  timezone=tmzn,
                  max_instances=1,
                  id="Precificacao_Ecommerce")

try:
    scheduler.start()
except (KeyboardInterrupt, SystemExit):
    pool.close()
    sys.exit(0)
except Exception as e: # pylint: disable=broad-exception-caught
    pool.close()
    logger.error(e)
    sys.exit(1)
