from pathlib import Path

from spark.iceberg_session import get_spark


def main():
    spark = get_spark("Nexus-Create-Repo-CDC-Bronze")

    try:
        sql_path = (
            Path(__file__).resolve().parents[2]
            / "sql"
            / "cdc"
            / "01_create_repo_cdc_bronze.sql"
        )

        sql_text = sql_path.read_text()

        for statement in sql_text.split(";"):
            statement = statement.strip()
            if statement:
                spark.sql(statement)

        print("\nBronze CDC table created successfully.")
        spark.sql(
            "DESCRIBE TABLE EXTENDED nexus.bronze.repo_cdc"
        ).show(100, truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
