
import os

from dotenv import load_dotenv
from pyspark.sql import Window
from pyspark.sql import functions as F

from spark.iceberg_session import get_spark


load_dotenv()


def seed_repos():
    spark = get_spark("Nexus-Repository-Seed")

    try:
        spark.conf.set("spark.sql.shuffle.partitions", "8")

        print("Reading Silver events...")

        events = spark.table("nexus.silver.events")

        # Extract valid repository identifiers and names.
        print("Extracting repository metadata...")

        repos = (
            events
            .filter(
                F.col("repo_id").isNotNull()
                & F.col("repo_name").isNotNull()
                & (F.trim(F.col("repo_name")) != "")
            )
            .select("repo_id", "repo_name")
            .withColumn("repo_id", F.col("repo_id").cast("long"))
            .filter(F.col("repo_id").isNotNull())
            .filter(F.col("repo_name").contains("/"))
            .withColumn(
                "owner",
                F.split(F.col("repo_name"), "/").getItem(0)
            )
            .withColumn(
                "name",
                F.element_at(F.split(F.col("repo_name"), "/"), -1)
            )
            .filter(
                (F.col("owner") != "")
                & (F.col("name") != "")
            )
        )

        # Resolve historical repository renames:
        # choose the most frequently observed name for each repo_id.
        print("Resolving repository name variations...")

        name_counts = (
            repos
            .groupBy("repo_id", "name", "owner")
            .count()
        )

        window = Window.partitionBy("repo_id").orderBy(
            F.col("count").desc(),
            F.col("name").asc(),
            F.col("owner").asc()
        )

        final_repos = (
            name_counts
            .withColumn(
                "_rank",
                F.row_number().over(window)
            )
            .filter(F.col("_rank") == 1)
            .select(
                "repo_id",
                "name",
                "owner",
                F.lit(0).cast("long").alias("stars"),
                F.lit(False).alias("is_archived"),
                F.current_timestamp().alias("updated_at")
            )
        )

        # Count before writing.
        repo_count = final_repos.count()

        print(f"Repositories identified: {repo_count:,}")

        if repo_count == 0:
            raise RuntimeError("No valid repositories found.")

        # Convert to a PostgreSQL-compatible staging table.
        staging_table = "public.repos_seed_staging"

        print("Writing repository data to PostgreSQL staging...")

        (
            final_repos.write
            .format("jdbc")
            .option("url", "jdbc:postgresql://localhost:5432/app")
            .option("dbtable", staging_table)
            .option("user", os.environ["POSTGRES_USER"])
            .option("password", os.environ["POSTGRES_PASSWORD"])
            .option("driver", "org.postgresql.Driver")
            .option("batchsize", "5000")
            .mode("overwrite")
            .save()
        )

        # Upsert into the operational table.
        # Existing repository records are preserved on conflict.
        print("Upserting repositories into app.repos...")

        import psycopg2

        connection = psycopg2.connect(
            host="localhost",
            port=5432,
            dbname="app",
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"],
        )

        try:
            with connection:
                with connection.cursor() as cursor:
                    cursor.execute("""
                        INSERT INTO public.repos (
                            repo_id,
                            name,
                            owner,
                            stars,
                            is_archived,
                            updated_at
                        )
                        SELECT
                            repo_id,
                            name,
                            owner,
                            stars,
                            is_archived,
                            updated_at
                        FROM public.repos_seed_staging
                        ON CONFLICT (repo_id)
                        DO NOTHING;
                    """)

                    inserted = cursor.rowcount

                    cursor.execute("""
                        SELECT COUNT(*)
                        FROM public.repos;
                    """)

                    total = cursor.fetchone()[0]

            print(f"New repositories inserted: {inserted:,}")
            print(f"Total repositories in app.repos: {total:,}")

        finally:
            connection.close()

        # Remove temporary staging table.
        print("Cleaning up staging table...")

        connection = psycopg2.connect(
            host="localhost",
            port=5432,
            dbname="app",
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"],
        )

        try:
            with connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        "DROP TABLE IF EXISTS public.repos_seed_staging;"
                    )
        finally:
            connection.close()

        print("Repository seeding completed successfully.")

    finally:
        spark.stop()


if __name__ == "__main__":
    seed_repos()
