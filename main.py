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
timezone = timezone('America/Sao_Paulo')
logger.info('start scheduler mm_worker!')
scheduler = BlockingScheduler()
scheduler.add_job(sales_disable,'cron', hour='7-20', minute='*', timezone=timezone)
scheduler.add_job(CustoMedio, 'cron', hour='7-19', minute="*/10", timezone=timezone)
scheduler.add_job(Precificacao, 'cron', day_of_week='mon-sat', hour='7-19', minute="*/15", timezone=timezone)
scheduler.add_job(atualizacao_search, 'cron', day_of_week='mon-fri', hour=20, minute=0, timezone=timezone)
scheduler.add_job(execucao_multi, 'cron', hour='7-19', minute="*/30", timezone=timezone)
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
