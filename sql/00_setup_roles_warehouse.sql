/* =============================================================================
   Lab 0 - Roles and warehouse
   Run as: ACCOUNTADMIN
   -----------------------------------------------------------------------------
   Role model used throughout the lab
     LAKEHOUSE_ADMIN   owns the database, Iceberg tables and policies
     DATA_ENGINEER     pipeline role: full read + DML on every layer, sees PII
     GROUP_DBX_READER  sister company on Databricks / Spark / Trino: SILVER + GOLD, PII masked
     GROUP_AWS_READER  sister company on Athena (Glue federation): GOLD only, PII masked,
                       limited regions (row access policy)
   ========================================================================== */
USE ROLE ACCOUNTADMIN;

CREATE WAREHOUSE IF NOT EXISTS LAKEHOUSE_WH
  WAREHOUSE_SIZE = XSMALL AUTO_SUSPEND = 60 AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE;

CREATE ROLE IF NOT EXISTS LAKEHOUSE_ADMIN  COMMENT = 'Owner of GROUP_LAKEHOUSE Iceberg tables and policies';
CREATE ROLE IF NOT EXISTS DATA_ENGINEER    COMMENT = 'Pipeline role that builds the medallion layers';
CREATE ROLE IF NOT EXISTS GROUP_DBX_READER COMMENT = 'Read-only role for group engines via Horizon IRC (Databricks/Spark/Trino)';
CREATE ROLE IF NOT EXISTS GROUP_AWS_READER COMMENT = 'Read-only role for AWS Glue federation / Athena';

-- Role hierarchy: admin can impersonate every lab role for testing
GRANT ROLE DATA_ENGINEER    TO ROLE LAKEHOUSE_ADMIN;
GRANT ROLE GROUP_DBX_READER TO ROLE LAKEHOUSE_ADMIN;
GRANT ROLE GROUP_AWS_READER TO ROLE LAKEHOUSE_ADMIN;
GRANT ROLE LAKEHOUSE_ADMIN  TO ROLE SYSADMIN;

GRANT CREATE DATABASE ON ACCOUNT TO ROLE LAKEHOUSE_ADMIN;
GRANT USAGE ON WAREHOUSE LAKEHOUSE_WH TO ROLE LAKEHOUSE_ADMIN;
GRANT USAGE ON WAREHOUSE LAKEHOUSE_WH TO ROLE DATA_ENGINEER;
-- Readers get a warehouse only so you can test them inside Snowsight;
-- external engines do NOT need a warehouse to read through Horizon IRC.
GRANT USAGE ON WAREHOUSE LAKEHOUSE_WH TO ROLE GROUP_DBX_READER;
GRANT USAGE ON WAREHOUSE LAKEHOUSE_WH TO ROLE GROUP_AWS_READER;

-- Give yourself the admin role
SET my_user = CURRENT_USER();
GRANT ROLE LAKEHOUSE_ADMIN TO USER IDENTIFIER($my_user);

-- Account identifier used by every external engine (format <org>-<account>)
SELECT CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT_NAME() AS account_identifier,
       CURRENT_REGION() AS region;
