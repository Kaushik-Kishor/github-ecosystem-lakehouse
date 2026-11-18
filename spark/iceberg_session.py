import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv()

ICEBERG_VERSION = "1.9.2"

PACKAGES = ",".join(
    [
        f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{ICEBERG_VERSION}",
        f"org.apache.iceberg:iceberg-aws-bundle:{ICEBERG_VERSION}",
        "org.postgresql:postgresql:42.7.5",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6",
        "org.apache.spark:spark-avro_2.12:3.5.6",
    ]
)


def get_spark(app_name: str = "Nexus-Iceberg"):
    required_env = [
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "MINIO_ENDPOINT",
        "AWS_DEFAULT_REGION",
        "S3_BUCKET",
    ]

    missing = [key for key in required_env if not os.getenv(key)]
    if missing:
        raise EnvironmentError(
            f"Missing environment variables: {', '.join(missing)}"
        )

    catalog = "nexus"

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[4]")
        .config("spark.jars.packages", PACKAGES)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(
            f"spark.sql.catalog.{catalog}",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config(f"spark.sql.catalog.{catalog}.type", "jdbc")
        .config(
            f"spark.sql.catalog.{catalog}.uri",
            "jdbc:postgresql://localhost:5432/iceberg",
        )
        .config(
            f"spark.sql.catalog.{catalog}.jdbc.user",
            os.environ["POSTGRES_USER"],
        )
        .config(
            f"spark.sql.catalog.{catalog}.jdbc.password",
            os.environ["POSTGRES_PASSWORD"],
        )
        .config(
            f"spark.sql.catalog.{catalog}.warehouse",
            f"s3://{os.environ['S3_BUCKET']}/warehouse",
        )
        .config(
            f"spark.sql.catalog.{catalog}.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO",
        )
        .config(
            f"spark.sql.catalog.{catalog}.s3.endpoint",
            os.environ["MINIO_ENDPOINT"],
        )
        .config(
            f"spark.sql.catalog.{catalog}.s3.path-style-access",
            "true",
        )
        .config(
            f"spark.sql.catalog.{catalog}.s3.access-key-id",
            os.environ["AWS_ACCESS_KEY_ID"],
        )
        .config(
            f"spark.sql.catalog.{catalog}.s3.secret-access-key",
            os.environ["AWS_SECRET_ACCESS_KEY"],
        )
        .config(
            f"spark.sql.catalog.{catalog}.client.region",
            os.environ["AWS_DEFAULT_REGION"],
        )
        .config("spark.sql.shuffle.partitions", "4")
    )

    return builder.getOrCreate()


if __name__ == "__main__":
    spark = get_spark()

    print("Spark version:", spark.version)

    spark.sql("CREATE NAMESPACE IF NOT EXISTS nexus.bronze")

    print("\nIceberg namespaces:")
    spark.sql("SHOW NAMESPACES IN nexus").show()

    spark.stop()
