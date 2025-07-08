"""Módulo de precificação e criação do multi-grupo preço MM."""
import sys
import traceback
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

# Sempre que feito o recarregamento do processo, zerar os cache base fiscal
# cache.evict(tag='Atacado')
# cache.evict(tag='Ecommerce')

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
    sys.exit(1)
except Exception as e: # pylint: disable=broad-exception-caught
    pool.close()
    logger.error(e)
    logger.error(traceback.format_exc())
    sys.exit(0)
