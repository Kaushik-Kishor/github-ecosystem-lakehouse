from pathlib import Path

from spark.iceberg_session import get_spark


def main():
    spark = get_spark("Nexus-Create-Silver-Repos-V2")

    try:
        sql_path = Path("sql/cdc/02_create_repos_silver_v2.sql")
        statements = [
            statement.strip()
            for statement in sql_path.read_text().split(";")
            if statement.strip()
        ]

        for statement in statements:
            spark.sql(statement)

        print("\nSilver v2 table created/verified:")
        spark.sql("DESCRIBE TABLE nexus.silver.repos_v2").show(
            50, truncate=False
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
