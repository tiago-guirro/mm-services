"""Módulo de precificação e criação do multi-grupo preço MM."""
import sys
from pytz import timezone
from apscheduler.schedulers.blocking import BlockingScheduler
from utils.log import logger
from pool_conn import pool
from precificacao import Precificacao
from search import atualizacao_search
from customedio import CustoMedio
from promocao import sales_disable
from precificacao_ecommerce import execucao_multi

tmzn = timezone('America/Sao_Paulo')
scheduler = BlockingScheduler()
scheduler.add_job(sales_disable,
                  'cron',
                  hour='7-20',
                  minute='*',
                  timezone=tmzn,
                  max_instances=1,
                  id="Desabilitar_Promocao",
                  name="Desabilitar_Promocao")
scheduler.add_job(CustoMedio,
                  'cron',
                  day_of_week='mon-sat',
                  hour='7-19',
                  minute="*/15",
                  timezone=tmzn,
                  max_instances=1,
                  id="Atualizar_Custo_Medio",
                  name="Atualizar_Custo_Medio")
scheduler.add_job(Precificacao,
                  'cron',
                  day_of_week='mon-sat',
                  hour='7-19',
                  minute="*/30",
                  timezone=tmzn,
                  max_instances=1,
                  id="Precificacao_Atacado",
                  name="Precificacao_Atacado"
                  )
scheduler.add_job(atualizacao_search,
                  'cron',
                  day_of_week='mon-sat',
                  hour='7-19',
                  minute='*/10',
                  timezone=tmzn,
                  max_instances=1,
                  id="Atualizacao_Pesquisa",
                  name="Atualizacao_Pesquisa")
scheduler.add_job(execucao_multi,
                  'cron',
                  day_of_week='mon-sat',
                  hour='7-19',
                  minute="*/30",
                  timezone=tmzn,
                  max_instances=1,
                  id="Precificacao_Ecommerce",
                  name="Precificacao_Ecommerce"
                  )

try:
    scheduler.start()
except (KeyboardInterrupt, SystemExit):
    pool.close()
    sys.exit(0)
except Exception as e: # pylint: disable=broad-exception-caught
    pool.close()
    logger.error(e)
    sys.exit(1)
