import duckdb

from config import DB_PATH, RAW_DIR, SQL_DIR, ensure_directories


def main():
    ensure_directories()
    sql = (SQL_DIR / "bronze" / "create_bronze_tables.sql").read_text()
    with duckdb.connect(DB_PATH) as con:
        con.execute(f"SET VARIABLE raw_path = '{RAW_DIR.as_posix()}';")
        con.execute(sql)
    print(f"Bronze layer loaded into {DB_PATH}")


if __name__ == "__main__":
    main()
