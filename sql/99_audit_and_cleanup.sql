/* =============================================================================
   Lab 8 - Audit external engine access + clean up
   ========================================================================== */
USE ROLE ACCOUNTADMIN;

-- Every Horizon IRC call (loadTable, PlanTableScan, ...) is recorded.
-- ACCOUNT_USAGE latency can be up to ~3 hours.
SELECT query_start_time, user_name,
       additional_properties:irc_event_type::STRING AS irc_event,
       direct_objects_accessed[0]:objectName::STRING AS object_name,
       policies_referenced
FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY
WHERE event_source = 'horizon_irc'
ORDER BY query_start_time DESC
LIMIT 50;

-- Who is consuming the data products, per company role
SELECT user_name, additional_properties:irc_event_type::STRING AS irc_event, COUNT(*) AS calls
FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY
WHERE event_source = 'horizon_irc'
  AND query_start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
GROUP BY ALL ORDER BY calls DESC;

/* ---------------------------------------------------------------------------
   CLEAN UP (uncomment to run). Dropping a Snowflake-managed Iceberg table
   removes its files from S3 after the retention period.
   ---------------------------------------------------------------------------
ALTER USER SVC_GROUP_DBX_READER UNSET AUTHENTICATION POLICY;
ALTER USER SVC_GROUP_AWS_READER UNSET AUTHENTICATION POLICY;
DROP USER IF EXISTS SVC_GROUP_DBX_READER;
DROP USER IF EXISTS SVC_GROUP_AWS_READER;
DROP DATABASE IF EXISTS GROUP_SHARE_AWS;
DROP DATABASE IF EXISTS GROUP_LAKEHOUSE;
DROP EXTERNAL VOLUME IF EXISTS LAKEHOUSE_EXT_VOL;
DROP ROLE IF EXISTS GROUP_AWS_READER;
DROP ROLE IF EXISTS GROUP_DBX_READER;
DROP ROLE IF EXISTS DATA_ENGINEER;
DROP ROLE IF EXISTS LAKEHOUSE_ADMIN;
DROP WAREHOUSE IF EXISTS LAKEHOUSE_WH;
*/
