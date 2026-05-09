import duckdb

from config import DB_PATH, SQL_DIR, TABLEAU_EXPORT_DIR, ensure_directories


GOLD_SQL_FILES = [
    "create_gold_kpi_tables.sql",
    "create_gold_funnel_tables.sql",
    "create_gold_experiment_tables.sql",
    "create_gold_segment_tables.sql",
]

EXPORT_TABLES = [
    "gold_daily_kpis",
    "gold_funnel_metrics",
    "gold_experiment_results",
    "gold_segment_performance",
]


def main():
    ensure_directories()
    with duckdb.connect(DB_PATH) as con:
        for file_name in GOLD_SQL_FILES:
            con.execute((SQL_DIR / "gold" / file_name).read_text())

        for table in EXPORT_TABLES:
            export_path = TABLEAU_EXPORT_DIR / f"{table}.csv"
            con.execute(f"COPY {table} TO '{export_path.as_posix()}' (HEADER, DELIMITER ',');")
            print(f"Exported {export_path}")

    print("Gold layer created and Tableau CSV exports refreshed")


if __name__ == "__main__":
    main()

