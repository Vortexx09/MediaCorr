import os
import re
from urllib.parse import urljoin

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def is_warc_gz(name: str) -> bool:
    return bool(re.match(r".*\.warc\.gz$", name))

def join_url(base: str, name: str) -> str:
    return urljoin(base, name.rstrip("/"))
