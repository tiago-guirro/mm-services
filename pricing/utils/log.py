"""Criando estrutura de log"""
import time
import sys
import os
import logging
from utils.cache_redis import cache

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(filename)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def log_redis(message, stype='notify'):
    """Gravando no redis o log do servidor"""
    if str(os.getenv('LOG','False')) == 'False':
        key = f"worker:production:{stype}:{int(time.time() * 1000)}"
        cache.client.set(key, message, ex=60*60*24*7)

def log_notify(message):
    """Enviando mensagem somente local"""
    if str(os.getenv('LOG','False')) == 'True':
        logger.info(message)

def log_error(message):
    """Enviando mensagem de erros"""
    log_redis(message,'error')
    logger.error(message)
