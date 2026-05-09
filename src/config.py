from pathlib import Path

from dotenv import load_dotenv


ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
TABLEAU_EXPORT_DIR = DATA_DIR / "tableau_exports"
DOCS_DIR = ROOT_DIR / "docs"
SQL_DIR = ROOT_DIR / "sql"
DB_PATH = PROCESSED_DIR / "ecommerce_analytics.duckdb"

load_dotenv(ROOT_DIR / ".env")


def ensure_directories():
    for path in [RAW_DIR, PROCESSED_DIR, TABLEAU_EXPORT_DIR, DOCS_DIR]:
        path.mkdir(parents=True, exist_ok=True)

