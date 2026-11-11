-- Lab 8 - Amazon Athena on the Glue federated catalog (catalog name: group-share-aws)
-- Athena console -> Data source: AwsDataCatalog -> Catalog: group-share-aws
-- Use UPPER CASE identifiers, quoted - Snowflake stores them upper case.

SELECT * FROM "group-share-aws"."SALES"."CUSTOMER_SEGMENT_SUMMARY" ORDER BY "REGION", "SEGMENT";

SELECT "REGION", "CHANNEL", SUM("REVENUE") AS revenue
FROM "group-share-aws"."SALES"."REGIONAL_SALES_DAILY"
WHERE "TXN_DATE" >= current_date - INTERVAL '30' DAY
GROUP BY 1, 2
ORDER BY revenue DESC;

-- Iceberg time travel works on the snapshots Snowflake committed
SELECT * FROM "group-share-aws"."SALES"."REGIONAL_SALES_DAILY$snapshots";

-- Redshift: query the same federated catalog from Redshift Query Editor v2 once the
-- Lake Formation grants include the Redshift role, e.g.
--   SELECT * FROM "group-share-aws@awsdatacatalog"."SALES"."REGIONAL_SALES_DAILY" LIMIT 10;
-- (Redshift support for Glue federated catalogs depends on region/version - verify first.)
