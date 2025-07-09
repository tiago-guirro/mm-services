"""Configuração do cache local com diskcache"""

from pathlib import Path
from diskcache import Cache

# Define o diretório raiz do projeto com base na localização deste arquivo
project_root = Path(__file__).resolve().parent

# Inicializa o cache persistente no diretório 'cache' dentro do projeto
# Isso permite armazenar dados entre execuções, ideal para ambientes locais ou desenvolvimento
cache = Cache(str(project_root / 'cache'))
