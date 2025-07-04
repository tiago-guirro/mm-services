""" Incorporando cache """
from pathlib import Path
from diskcache import Cache
project_root = Path(__file__).resolve().parent
cache = Cache(str(project_root / 'cache'))
