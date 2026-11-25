import os
from pathlib import Path


DATA_DIR = Path(os.environ.get("ADVDB_DATA_DIR") or Path.home() / "advdb").expanduser().resolve()
RAW_DIR = DATA_DIR / "raw"
PARQUET_DIR = DATA_DIR / "parquet"
