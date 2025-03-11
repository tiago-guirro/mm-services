"""Módulo de precificação e criação do multi-grupo preço MM."""
import sys
import os
import time
import logging
import traceback
import schedule
from pricing.pool_conn import pool
from pricing.precificacao import Precificacao
from pricing.search import atualizacao_search
from pricing.customedio import CustoMedio

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

# schedule.every().day.at("22:00", 'America/Sao_Paulo').do(
#     atualizacao_search,
#     pool,
#     logger)
schedule.every(
    int(os.getenv("SCHEDULER_CUSTOMEDIO",'1'))
    ).seconds.do(
    CustoMedio,
    pool,
    logger)
# schedule.every(
#     int(os.getenv("SCHEDULER_PRECIFICACAO",'10'))
#     ).minutes.do(
#     Precificacao,
#     pool,
#     logger)

logger.info('start mm_worker!')

while True:
    try:
        schedule.run_pending()
    except Exception as e: # pylint: disable=broad-exception-caught
        logger.error(str(e))
        print(traceback.format_exc())
    except KeyboardInterrupt:
        pool.close()
        sys.exit(0)
    finally:
        time.sleep(1)
