/* =============================================================================
   Lab 3 - Data governance with Snowflake Horizon: RBAC + masking + row access
   Run as: LAKEHOUSE_ADMIN
   -----------------------------------------------------------------------------
   Everything defined here is enforced:
     * inside Snowflake (Snowsight, BI tools, Cortex, Streamlit), and
     * for external engines reading through the Horizon Iceberg REST Catalog
       (RBAC always; masking/row access via the Scan Plan API - see README).
   Policy predicates below deliberately use the patterns Snowflake documents as
   supported for Scan Plan enforcement (CURRENT_ROLE() IN (...), EXISTS on a
   mapping table).
   ========================================================================== */
USE ROLE LAKEHOUSE_ADMIN;
USE WAREHOUSE LAKEHOUSE_WH;
USE SCHEMA GROUP_LAKEHOUSE.GOVERNANCE;

-- ---------------------------------------------------------------------------
-- 3.1 RBAC - who can see which layer
-- ---------------------------------------------------------------------------
-- Pipeline role: everything, including DML on all layers
GRANT USAGE ON DATABASE GROUP_LAKEHOUSE TO ROLE DATA_ENGINEER;
GRANT USAGE ON ALL SCHEMAS IN DATABASE GROUP_LAKEHOUSE TO ROLE DATA_ENGINEER;
GRANT CREATE ICEBERG TABLE ON SCHEMA GROUP_LAKEHOUSE.SILVER TO ROLE DATA_ENGINEER;
GRANT CREATE ICEBERG TABLE ON SCHEMA GROUP_LAKEHOUSE.GOLD   TO ROLE DATA_ENGINEER;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL ICEBERG TABLES IN DATABASE GROUP_LAKEHOUSE TO ROLE DATA_ENGINEER;

-- Databricks / Spark / Trino sister company: SILVER + GOLD, read only
GRANT USAGE ON DATABASE GROUP_LAKEHOUSE TO ROLE GROUP_DBX_READER;
GRANT USAGE ON SCHEMA GROUP_LAKEHOUSE.SILVER TO ROLE GROUP_DBX_READER;
GRANT USAGE ON SCHEMA GROUP_LAKEHOUSE.GOLD   TO ROLE GROUP_DBX_READER;
GRANT SELECT ON ALL ICEBERG TABLES    IN SCHEMA GROUP_LAKEHOUSE.SILVER TO ROLE GROUP_DBX_READER;
GRANT SELECT ON ALL ICEBERG TABLES    IN SCHEMA GROUP_LAKEHOUSE.GOLD   TO ROLE GROUP_DBX_READER;
GRANT SELECT ON FUTURE ICEBERG TABLES IN SCHEMA GROUP_LAKEHOUSE.SILVER TO ROLE GROUP_DBX_READER;
GRANT SELECT ON FUTURE ICEBERG TABLES IN SCHEMA GROUP_LAKEHOUSE.GOLD   TO ROLE GROUP_DBX_READER;

-- Athena / Glue sister company: GOLD only, read only
GRANT USAGE ON DATABASE GROUP_LAKEHOUSE TO ROLE GROUP_AWS_READER;
GRANT USAGE ON SCHEMA GROUP_LAKEHOUSE.GOLD TO ROLE GROUP_AWS_READER;
GRANT SELECT ON ALL ICEBERG TABLES    IN SCHEMA GROUP_LAKEHOUSE.GOLD TO ROLE GROUP_AWS_READER;
GRANT SELECT ON FUTURE ICEBERG TABLES IN SCHEMA GROUP_LAKEHOUSE.GOLD TO ROLE GROUP_AWS_READER;
-- BRONZE is never granted to group roles -> it is invisible to every external engine.

-- ---------------------------------------------------------------------------
-- 3.2 Tag + masking policies for PII
-- ---------------------------------------------------------------------------
CREATE TAG IF NOT EXISTS PII_TYPE ALLOWED_VALUES 'EMAIL', 'PHONE', 'NATIONAL_ID', 'NAME'
  COMMENT = 'Sensitive data classification';

CREATE OR REPLACE MASKING POLICY MASK_PII_STRING AS (val STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('LAKEHOUSE_ADMIN', 'DATA_ENGINEER') THEN val
    ELSE '***MASKED***'
  END
  COMMENT = 'Full value for pipeline roles, fully masked for everyone else';

CREATE OR REPLACE MASKING POLICY MASK_EMAIL AS (val STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('LAKEHOUSE_ADMIN', 'DATA_ENGINEER') THEN val
    ELSE REGEXP_REPLACE(val, '^[^@]+', '*****')      -- keeps the domain for analytics
  END
  COMMENT = 'Keeps e-mail domain, hides the local part';

-- SILVER.CUSTOMERS
ALTER ICEBERG TABLE GROUP_LAKEHOUSE.SILVER.CUSTOMERS MODIFY COLUMN email SET MASKING POLICY MASK_EMAIL;
ALTER ICEBERG TABLE GROUP_LAKEHOUSE.SILVER.CUSTOMERS MODIFY COLUMN phone SET MASKING POLICY MASK_PII_STRING;
ALTER ICEBERG TABLE GROUP_LAKEHOUSE.SILVER.CUSTOMERS MODIFY COLUMN nik   SET MASKING POLICY MASK_PII_STRING;
ALTER ICEBERG TABLE GROUP_LAKEHOUSE.SILVER.CUSTOMERS MODIFY COLUMN email SET TAG PII_TYPE = 'EMAIL';
ALTER ICEBERG TABLE GROUP_LAKEHOUSE.SILVER.CUSTOMERS MODIFY COLUMN phone SET TAG PII_TYPE = 'PHONE';
ALTER ICEBERG TABLE GROUP_LAKEHOUSE.SILVER.CUSTOMERS MODIFY COLUMN nik   SET TAG PII_TYPE = 'NATIONAL_ID';

-- GOLD.CUSTOMER_360
ALTER ICEBERG TABLE GROUP_LAKEHOUSE.GOLD.CUSTOMER_360 MODIFY COLUMN email SET MASKING POLICY MASK_EMAIL;
ALTER ICEBERG TABLE GROUP_LAKEHOUSE.GOLD.CUSTOMER_360 MODIFY COLUMN phone SET MASKING POLICY MASK_PII_STRING;
ALTER ICEBERG TABLE GROUP_LAKEHOUSE.GOLD.CUSTOMER_360 MODIFY COLUMN email SET TAG PII_TYPE = 'EMAIL';
ALTER ICEBERG TABLE GROUP_LAKEHOUSE.GOLD.CUSTOMER_360 MODIFY COLUMN phone SET TAG PII_TYPE = 'PHONE';

-- ---------------------------------------------------------------------------
-- 3.3 Row access policy - the AWS company only sees selected regions
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ROLE_REGION_MAP (role_name STRING, region STRING);
DELETE FROM ROLE_REGION_MAP;
INSERT INTO ROLE_REGION_MAP VALUES
  ('GROUP_AWS_READER', 'JAKARTA'),
  ('GROUP_AWS_READER', 'JAWA BARAT');

CREATE OR REPLACE ROW ACCESS POLICY RAP_REGION AS (row_region STRING) RETURNS BOOLEAN ->
  CURRENT_ROLE() IN ('LAKEHOUSE_ADMIN', 'DATA_ENGINEER', 'GROUP_DBX_READER')
  OR EXISTS (
       SELECT 1 FROM GROUP_LAKEHOUSE.GOVERNANCE.ROLE_REGION_MAP m
       WHERE m.role_name = CURRENT_ROLE() AND m.region = row_region)
  COMMENT = 'Full rows for internal + DBX roles; mapped regions for other roles';

ALTER ICEBERG TABLE GROUP_LAKEHOUSE.GOLD.CUSTOMER_360         ADD ROW ACCESS POLICY RAP_REGION ON (region);
ALTER ICEBERG TABLE GROUP_LAKEHOUSE.GOLD.REGIONAL_SALES_DAILY ADD ROW ACCESS POLICY RAP_REGION ON (region);

-- ---------------------------------------------------------------------------
-- 3.4 Test inside Snowflake - same rules will apply to external engines
-- ---------------------------------------------------------------------------
-- IMPORTANT: users often have DEFAULT_SECONDARY_ROLES = ('ALL'). Secondary roles
-- add privileges, so a GRANT test would wrongly succeed. Engines connecting
-- through Horizon IRC use only the scoped role, so mimic that here:
USE SECONDARY ROLES NONE;
USE ROLE DATA_ENGINEER;
SELECT customer_id, email, phone, region FROM GROUP_LAKEHOUSE.GOLD.CUSTOMER_360 ORDER BY customer_id LIMIT 5;  -- clear text

USE ROLE GROUP_DBX_READER;
SELECT customer_id, email, phone, region FROM GROUP_LAKEHOUSE.GOLD.CUSTOMER_360 ORDER BY customer_id LIMIT 5;  -- masked, all regions

USE ROLE GROUP_AWS_READER;
SELECT region, COUNT(*) FROM GROUP_LAKEHOUSE.GOLD.CUSTOMER_360 GROUP BY region;                               -- masked, 2 regions only
SELECT COUNT(*) FROM GROUP_LAKEHOUSE.SILVER.CUSTOMERS;                                                         -- fails: no grant on SILVER

USE ROLE LAKEHOUSE_ADMIN;
USE SECONDARY ROLES ALL;
SELECT * FROM TABLE(GROUP_LAKEHOUSE.INFORMATION_SCHEMA.POLICY_REFERENCES(
  ref_entity_name => 'GROUP_LAKEHOUSE.GOLD.CUSTOMER_360', ref_entity_domain => 'table'));
