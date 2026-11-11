/* =============================================================================
   Lab 5 - A dedicated share database for AWS Glue catalog federation (Athena)
   Run as: LAKEHOUSE_ADMIN (then ACCOUNTADMIN for the last block)
   -----------------------------------------------------------------------------
   WHY A SEPARATE DATABASE?
   AWS Glue catalog federation reads table METADATA through Horizon IRC, but reads
   the DATA FILES with the IAM role you register in Lake Formation. Athena does not
   call the Scan Plan API, so Snowflake masking / row access policies are NOT
   evaluated on this path. Fine-grained control there is Lake Formation's job.

   The safe pattern is therefore "publish, don't expose":
     * build a purpose-made data product that already contains only what the
       AWS company may see (no PII, only its regions),
     * put it in its own database (a Glue federated catalog maps to ONE database),
     * on its own S3 prefix, so the Lake Formation IAM role can be scoped to that
       prefix and can never read GROUP_LAKEHOUSE files.
   ========================================================================== */
USE ROLE LAKEHOUSE_ADMIN;
USE WAREHOUSE LAKEHOUSE_WH;

CREATE DATABASE IF NOT EXISTS GROUP_SHARE_AWS
  CATALOG = 'SNOWFLAKE'
  EXTERNAL_VOLUME = 'LAKEHOUSE_EXT_VOL'
  COMMENT = 'Data products published to the AWS (Athena/Redshift) sister company';
-- Athena reads Iceberg format v2. Pin it so a future account default cannot break Athena.
ALTER DATABASE GROUP_SHARE_AWS SET ICEBERG_VERSION_DEFAULT = 2;
CREATE SCHEMA IF NOT EXISTS GROUP_SHARE_AWS.SALES;

-- A Dynamic Iceberg Table: Snowflake keeps it fresh from GOLD, no pipeline code.
-- The region filter replaces the row access policy, and no PII column is projected.
CREATE OR REPLACE DYNAMIC ICEBERG TABLE GROUP_SHARE_AWS.SALES.REGIONAL_SALES_DAILY
  TARGET_LAG = '1 hour'
  WAREHOUSE = LAKEHOUSE_WH
  BASE_LOCATION = 'share_aws/sales/regional_sales_daily'
AS
SELECT txn_date, region, channel, product, txn_count, revenue
FROM GROUP_LAKEHOUSE.GOLD.REGIONAL_SALES_DAILY
WHERE region IN ('JAKARTA', 'JAWA BARAT');

CREATE OR REPLACE DYNAMIC ICEBERG TABLE GROUP_SHARE_AWS.SALES.CUSTOMER_SEGMENT_SUMMARY
  TARGET_LAG = '1 hour'
  WAREHOUSE = LAKEHOUSE_WH
  BASE_LOCATION = 'share_aws/sales/customer_segment_summary'
AS
SELECT region, segment,
       COUNT(*)                        AS customers,
       SUM(txn_count)                  AS txn_count,
       SUM(total_spend)::NUMBER(18,2)  AS total_spend
FROM GROUP_LAKEHOUSE.GOLD.CUSTOMER_360
WHERE region IN ('JAKARTA', 'JAWA BARAT')
GROUP BY ALL;

SHOW ICEBERG TABLES IN DATABASE GROUP_SHARE_AWS;
SELECT * FROM GROUP_SHARE_AWS.SALES.CUSTOMER_SEGMENT_SUMMARY ORDER BY region, segment;

-- The federation role sees ONLY this database.
GRANT USAGE ON DATABASE GROUP_SHARE_AWS TO ROLE GROUP_AWS_READER;
GRANT USAGE ON SCHEMA   GROUP_SHARE_AWS.SALES TO ROLE GROUP_AWS_READER;
GRANT SELECT ON ALL ICEBERG TABLES    IN SCHEMA GROUP_SHARE_AWS.SALES TO ROLE GROUP_AWS_READER;
GRANT SELECT ON FUTURE ICEBERG TABLES IN SCHEMA GROUP_SHARE_AWS.SALES TO ROLE GROUP_AWS_READER;
GRANT SELECT ON ALL DYNAMIC TABLES    IN SCHEMA GROUP_SHARE_AWS.SALES TO ROLE GROUP_AWS_READER;
GRANT SELECT ON FUTURE DYNAMIC TABLES IN SCHEMA GROUP_SHARE_AWS.SALES TO ROLE GROUP_AWS_READER;

-- Optional hardening: the AWS role no longer needs the protected GOLD tables at all.
-- REVOKE SELECT ON ALL ICEBERG TABLES IN SCHEMA GROUP_LAKEHOUSE.GOLD FROM ROLE GROUP_AWS_READER;

-- Files Athena will read live under this prefix -> scope the Lake Formation role to it
SELECT PARSE_JSON(SYSTEM$GET_ICEBERG_TABLE_INFORMATION(
         'GROUP_SHARE_AWS.SALES.REGIONAL_SALES_DAILY')):metadataLocation::STRING AS metadata_location;
