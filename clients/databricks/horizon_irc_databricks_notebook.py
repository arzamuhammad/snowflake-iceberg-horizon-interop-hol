# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 7 - Databricks on AWS reads Snowflake-managed Iceberg through Horizon IRC
# MAGIC
# MAGIC Databricks is plain Apache Spark here: the cluster registers Snowflake Horizon as an
# MAGIC **Iceberg REST catalog** named `horizon`. No data is copied and no Unity Catalog
# MAGIC federation is involved (Snowflake lists Unity Catalog federation to Horizon as
# MAGIC "not announced"), so this works today on any classic cluster.
# MAGIC
# MAGIC ## Cluster setup (one time, by the Databricks admin)
# MAGIC 1. **Compute → Create compute**
# MAGIC    * Databricks Runtime **15.4 LTS or 16.4 LTS** (Spark 3.5, Scala 2.12) - *not* serverless
# MAGIC    * Access mode **Dedicated (single user)** - custom Spark catalogs and Maven jars are
# MAGIC      not allowed on Standard/shared or serverless compute
# MAGIC 2. **Libraries → Install new → Maven**
# MAGIC    * `org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0`
# MAGIC    * `org.apache.iceberg:iceberg-aws-bundle:1.11.0`
# MAGIC 3. Store the PAT of `SVC_GROUP_DBX_READER` in a secret scope:
# MAGIC    ```
# MAGIC    databricks secrets create-scope horizon
# MAGIC    databricks secrets put-secret horizon dbx_reader_pat
# MAGIC    ```
# MAGIC 4. **Advanced options → Spark config** (catalogs must be declared at cluster start;
# MAGIC    replace `<org>-<account>` and the region):
# MAGIC    ```
# MAGIC    spark.sql.extensions org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
# MAGIC    spark.sql.catalog.horizon org.apache.iceberg.spark.SparkCatalog
# MAGIC    spark.sql.catalog.horizon.type rest
# MAGIC    spark.sql.catalog.horizon.uri https://<org>-<account>.snowflakecomputing.com/polaris/api/catalog
# MAGIC    spark.sql.catalog.horizon.warehouse GROUP_LAKEHOUSE
# MAGIC    spark.sql.catalog.horizon.credential {{secrets/horizon/dbx_reader_pat}}
# MAGIC    spark.sql.catalog.horizon.scope session:role:GROUP_DBX_READER
# MAGIC    spark.sql.catalog.horizon.header.X-Iceberg-Access-Delegation vended-credentials
# MAGIC    spark.sql.catalog.horizon.client.region ap-southeast-3
# MAGIC    spark.sql.iceberg.vectorization.enabled false
# MAGIC    ```
# MAGIC 5. If the Snowflake account has a network policy, allow the workspace's NAT egress IPs.
# MAGIC
# MAGIC > Tested path: the identical configuration was validated with OSS Spark 3.5 + Iceberg 1.11
# MAGIC > (see `clients/spark`). Databricks-specific packaging can differ between runtimes; if
# MAGIC > the Iceberg extension clashes with the runtime, test on the newest LTS first.

# COMMAND ----------

# The catalog is three-level: horizon.<SCHEMA>.<TABLE>  (Snowflake database = "warehouse")
display(spark.sql("SHOW NAMESPACES IN horizon"))   # GOLD, SILVER - BRONZE is never granted

# COMMAND ----------

display(spark.sql("SHOW TABLES IN horizon.GOLD"))

# COMMAND ----------

# MAGIC %md ### Unprotected Silver table - read with S3 credentials vended by Horizon

# COMMAND ----------

display(spark.sql("""
    SELECT region, channel, COUNT(*) AS txns, SUM(amount) AS revenue
    FROM horizon.SILVER.TRANSACTIONS
    GROUP BY region, channel
    ORDER BY revenue DESC
"""))

# COMMAND ----------

# MAGIC %md ### Protected Gold table - Horizon enforces the Snowflake masking policy
# MAGIC Expect `*****@example.com` and `***MASKED***`: Horizon plans the scan (Scan Plan API)
# MAGIC and hands Spark an already-masked dataset. The policy lives only in Snowflake.

# COMMAND ----------

display(spark.sql("""
    SELECT customer_id, email, phone, region, total_spend
    FROM horizon.GOLD.CUSTOMER_360
    ORDER BY customer_id
    LIMIT 10
"""))

# COMMAND ----------

# MAGIC %md ### Use the shared data with Databricks-native tables
# MAGIC Join Snowflake gold data with a Delta table the sister company owns, or land a copy
# MAGIC in Unity Catalog if they need it offline. The source of truth stays in Snowflake.

# COMMAND ----------

regional = spark.table("horizon.GOLD.REGIONAL_SALES_DAILY")
display(regional.groupBy("region").sum("revenue").orderBy("region"))

# Optional: materialise into Unity Catalog (requires a UC-enabled cluster + CREATE TABLE rights)
# regional.write.mode("overwrite").saveAsTable("main.group_share.regional_sales_daily")

# COMMAND ----------

# MAGIC %md ### Write test - must FAIL
# MAGIC `GROUP_DBX_READER` only has SELECT, so Horizon rejects the commit.

# COMMAND ----------

try:
    spark.sql("INSERT INTO horizon.SILVER.TRANSACTIONS SELECT * FROM horizon.SILVER.TRANSACTIONS LIMIT 1")
except Exception as e:
    print("Blocked as expected:", str(e)[:300])
