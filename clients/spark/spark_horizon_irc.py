"""
Lab 6b - Apache Spark (OSS Spark, Amazon EMR, AWS Glue jobs, SageMaker Spark) reading
Snowflake-managed Iceberg tables through the Horizon Iceberg REST Catalog.

    pip install "pyspark==3.5.*"
    export SNOWFLAKE_ACCOUNT=<org>-<account>
    export SNOWFLAKE_PAT=<PAT of SVC_GROUP_DBX_READER>
    export AWS_REGION=<region of the S3 bucket, e.g. ap-southeast-3>
    python spark_horizon_irc.py

Iceberg 1.11+ is required: it is the first release whose REST client uses the Scan Plan
API when the catalog asks for it (scan-planning-mode=server), which is how Horizon
enforces masking / row access policies for Spark. Older runtimes can still read
UNPROTECTED tables but get no credentials for protected ones.
"""
import os

from pyspark.sql import SparkSession

ACCOUNT = os.environ["SNOWFLAKE_ACCOUNT"]
PAT = os.environ["SNOWFLAKE_PAT"]
ROLE = os.getenv("SNOWFLAKE_ROLE", "GROUP_DBX_READER")
DATABASE = os.getenv("SNOWFLAKE_DATABASE", "GROUP_LAKEHOUSE")  # UPPER CASE
REGION = os.getenv("AWS_REGION", "ap-southeast-3")
ICEBERG_VERSION = "1.11.0"
CATALOG = "horizon"  # Spark-side alias for the Snowflake database

spark = (
    SparkSession.builder.appName("HorizonIRC")
    .master("local[*]")
    # local-mode only: avoid binding to VPN interfaces (remove on a real cluster)
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.jars.packages",
            f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{ICEBERG_VERSION},"
            f"org.apache.iceberg:iceberg-aws-bundle:{ICEBERG_VERSION}")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.defaultCatalog", CATALOG)
    .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
    .config(f"spark.sql.catalog.{CATALOG}.type", "rest")
    .config(f"spark.sql.catalog.{CATALOG}.uri", f"https://{ACCOUNT}.snowflakecomputing.com/polaris/api/catalog")
    .config(f"spark.sql.catalog.{CATALOG}.warehouse", DATABASE)
    .config(f"spark.sql.catalog.{CATALOG}.credential", PAT)
    .config(f"spark.sql.catalog.{CATALOG}.scope", f"session:role:{ROLE}")
    .config(f"spark.sql.catalog.{CATALOG}.client.region", REGION)
    .config(f"spark.sql.catalog.{CATALOG}.header.X-Iceberg-Access-Delegation", "vended-credentials")
    .config("spark.sql.iceberg.vectorization.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

spark.sql("SHOW NAMESPACES").show()                 # GOLD, SILVER only - BRONZE is not granted
spark.sql("SHOW TABLES IN GOLD").show()

# Unprotected table -> normal client-side planning with vended S3 credentials
spark.sql("""
    SELECT region, channel, COUNT(*) AS txns, SUM(amount) AS revenue
    FROM SILVER.TRANSACTIONS GROUP BY region, channel ORDER BY revenue DESC LIMIT 5
""").show()

# Protected table -> Horizon plans the scan and returns a masked / filtered dataset
spark.sql("""
    SELECT customer_id, email, phone, region, total_spend
    FROM GOLD.CUSTOMER_360 ORDER BY customer_id LIMIT 5
""").show(truncate=False)
spark.sql("SELECT region, COUNT(*) AS customers FROM GOLD.CUSTOMER_360 GROUP BY region ORDER BY 1").show()
