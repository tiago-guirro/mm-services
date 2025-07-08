"""Criando estrutura de log"""
import sys
import os
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(filename)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def log_notify(message):
    """Enviando mensagem somente local"""
    if bool(os.getenv('LOG','False')):
        logger.info(message)

def log_error(message):
    """Enviando mensagem de erros"""
    logger.error(message)
