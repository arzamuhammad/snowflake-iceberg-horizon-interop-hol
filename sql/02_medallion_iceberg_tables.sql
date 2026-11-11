/* =============================================================================
   Lab 2 - Medallion architecture on Snowflake-managed Iceberg tables
   Run as: LAKEHOUSE_ADMIN
   -----------------------------------------------------------------------------
   * CATALOG = 'SNOWFLAKE'      -> Snowflake (Horizon) is the Iceberg catalog
   * EXTERNAL_VOLUME            -> data + metadata files land in YOUR S3 bucket
   * Snowflake performs commits, compaction and snapshot expiry for you.
   Setting both at database level means every Iceberg table inherits them.
   ========================================================================== */
USE ROLE LAKEHOUSE_ADMIN;
USE WAREHOUSE LAKEHOUSE_WH;

CREATE DATABASE IF NOT EXISTS GROUP_LAKEHOUSE
  CATALOG = 'SNOWFLAKE'
  EXTERNAL_VOLUME = 'LAKEHOUSE_EXT_VOL'
  COMMENT = 'Subsidiary medallion lakehouse - Snowflake-managed Iceberg on customer S3';

CREATE SCHEMA IF NOT EXISTS GROUP_LAKEHOUSE.BRONZE;
CREATE SCHEMA IF NOT EXISTS GROUP_LAKEHOUSE.SILVER;
CREATE SCHEMA IF NOT EXISTS GROUP_LAKEHOUSE.GOLD;
CREATE SCHEMA IF NOT EXISTS GROUP_LAKEHOUSE.GOVERNANCE;   -- policies, tags, mapping tables (native objects)

-- ---------------------------------------------------------------------------
-- BRONZE : raw, append-only. In production these are loaded from your S3 landing
-- zone with COPY INTO / Snowpipe (orchestrated by Airflow/MWAA or Snowflake Tasks).
-- For the lab we generate synthetic rows so no source system is required.
-- Iceberg note: use TIMESTAMP_NTZ(6) (microseconds) - Iceberg has no nanosecond type in v2.
-- ---------------------------------------------------------------------------
CREATE ICEBERG TABLE IF NOT EXISTS GROUP_LAKEHOUSE.BRONZE.RAW_CUSTOMERS (
    customer_id  INT,
    full_name    STRING,
    email        STRING,
    phone        STRING,
    nik          STRING,          -- national ID number (PII)
    city         STRING,
    region       STRING,
    segment      STRING,
    created_at   TIMESTAMP_NTZ(6),
    _loaded_at   TIMESTAMP_NTZ(6)
)
BASE_LOCATION = 'bronze/raw_customers';

CREATE ICEBERG TABLE IF NOT EXISTS GROUP_LAKEHOUSE.BRONZE.RAW_TRANSACTIONS (
    txn_id       INT,
    customer_id  INT,
    txn_ts       TIMESTAMP_NTZ(6),
    amount       NUMBER(18,2),
    channel      STRING,
    product      STRING,
    _loaded_at   TIMESTAMP_NTZ(6)
)
BASE_LOCATION = 'bronze/raw_transactions';

INSERT INTO GROUP_LAKEHOUSE.BRONZE.RAW_CUSTOMERS
SELECT
    SEQ4() + 1                                                         AS customer_id,
    '  Customer ' || (SEQ4() + 1) || '  '                              AS full_name,   -- untrimmed on purpose
    'User' || (SEQ4() + 1) || '@Example.com'                           AS email,       -- mixed case on purpose
    '+62812' || LPAD((SEQ4() * 7919) % 10000000, 7, '0')               AS phone,
    '3171' || LPAD((SEQ4() * 104729) % 1000000000000, 12, '0')         AS nik,
    DECODE(UNIFORM(1, 6, RANDOM(11)), 1,'Jakarta',2,'Bandung',3,'Surabaya',4,'Medan',5,'Balikpapan','Makassar') AS city,
    DECODE(UNIFORM(1, 6, RANDOM(11)), 1,'JAKARTA',2,'JAWA BARAT',3,'JAWA TIMUR',4,'SUMATERA',5,'KALIMANTAN','SULAWESI') AS region,
    DECODE(UNIFORM(1, 3, RANDOM(12)), 1,'RETAIL',2,'SME','CORPORATE') AS segment,
    DATEADD('day', -UNIFORM(30, 1500, RANDOM(13)), CURRENT_TIMESTAMP())::TIMESTAMP_NTZ(6) AS created_at,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(6)                              AS _loaded_at
FROM TABLE(GENERATOR(ROWCOUNT => 5000));

-- a few duplicates to show Silver de-duplication
INSERT INTO GROUP_LAKEHOUSE.BRONZE.RAW_CUSTOMERS
SELECT * FROM GROUP_LAKEHOUSE.BRONZE.RAW_CUSTOMERS WHERE customer_id <= 50;

INSERT INTO GROUP_LAKEHOUSE.BRONZE.RAW_TRANSACTIONS
SELECT
    SEQ4() + 1                                                         AS txn_id,
    UNIFORM(1, 5000, RANDOM(21))                                       AS customer_id,
    DATEADD('minute', -UNIFORM(1, 60*24*90, RANDOM(22)), CURRENT_TIMESTAMP())::TIMESTAMP_NTZ(6) AS txn_ts,
    ROUND(UNIFORM(10000, 5000000, RANDOM(23)) / 1, 2)::NUMBER(18,2)    AS amount,     -- IDR
    DECODE(UNIFORM(1, 4, RANDOM(24)), 1,'MOBILE',2,'WEB',3,'BRANCH','PARTNER') AS channel,
    DECODE(UNIFORM(1, 4, RANDOM(25)), 1,'SAVINGS',2,'CREDIT_CARD',3,'INSURANCE','LOAN') AS product,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(6)                              AS _loaded_at
FROM TABLE(GENERATOR(ROWCOUNT => 100000));

-- ---------------------------------------------------------------------------
-- SILVER : cleansed + conformed. In production this is where your transformation
-- tool (dbt Projects on Snowflake, Tasks, Snowpark, Dynamic Tables) writes - the
-- important part for interoperability is only that the target is an Iceberg table.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE ICEBERG TABLE GROUP_LAKEHOUSE.SILVER.CUSTOMERS
  BASE_LOCATION = 'silver/customers'
AS
SELECT customer_id,
       TRIM(full_name)  AS full_name,
       LOWER(email)     AS email,
       phone,
       nik,
       INITCAP(city)    AS city,
       UPPER(region)    AS region,
       segment,
       created_at
FROM GROUP_LAKEHOUSE.BRONZE.RAW_CUSTOMERS
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY _loaded_at DESC) = 1;

CREATE OR REPLACE ICEBERG TABLE GROUP_LAKEHOUSE.SILVER.TRANSACTIONS
  BASE_LOCATION = 'silver/transactions'
AS
SELECT t.txn_id, t.customer_id, t.txn_ts, t.txn_ts::DATE AS txn_date,
       t.amount, t.channel, t.product, c.region
FROM GROUP_LAKEHOUSE.BRONZE.RAW_TRANSACTIONS t
JOIN GROUP_LAKEHOUSE.SILVER.CUSTOMERS c USING (customer_id)
WHERE t.amount > 0
QUALIFY ROW_NUMBER() OVER (PARTITION BY t.txn_id ORDER BY t._loaded_at DESC) = 1;

-- ---------------------------------------------------------------------------
-- GOLD : business-ready data products shared with the group
-- ---------------------------------------------------------------------------
CREATE OR REPLACE ICEBERG TABLE GROUP_LAKEHOUSE.GOLD.CUSTOMER_360
  BASE_LOCATION = 'gold/customer_360'
AS
SELECT c.customer_id, c.full_name, c.email, c.phone, c.city, c.region, c.segment,
       COUNT(t.txn_id)               AS txn_count,
       COALESCE(SUM(t.amount), 0)::NUMBER(18,2) AS total_spend,
       MAX(t.txn_ts)                 AS last_txn_ts
FROM GROUP_LAKEHOUSE.SILVER.CUSTOMERS c
LEFT JOIN GROUP_LAKEHOUSE.SILVER.TRANSACTIONS t USING (customer_id)
GROUP BY ALL;

CREATE OR REPLACE ICEBERG TABLE GROUP_LAKEHOUSE.GOLD.REGIONAL_SALES_DAILY
  BASE_LOCATION = 'gold/regional_sales_daily'
AS
SELECT txn_date, region, channel, product,
       COUNT(*)                    AS txn_count,
       SUM(amount)::NUMBER(18,2)   AS revenue
FROM GROUP_LAKEHOUSE.SILVER.TRANSACTIONS
GROUP BY ALL;

-- Check: every table is Iceberg, catalog SNOWFLAKE, on your external volume
SHOW ICEBERG TABLES IN DATABASE GROUP_LAKEHOUSE;

-- The Iceberg metadata file external engines will read (lives in your S3 bucket)
SELECT SYSTEM$GET_ICEBERG_TABLE_INFORMATION('GROUP_LAKEHOUSE.GOLD.CUSTOMER_360');
