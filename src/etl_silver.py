import duckdb

from config import DB_PATH, SQL_DIR


def main():
    sql = (SQL_DIR / "silver" / "create_silver_tables.sql").read_text()
    with duckdb.connect(DB_PATH) as con:
        con.execute(sql)
    print("Silver layer created")


if __name__ == "__main__":
    main()

