-- Lab 6c - Trino queries (run inside: docker exec -it trino-horizon trino --catalog horizon)
SHOW SCHEMAS;                                   -- gold, silver (BRONZE is not granted)
SHOW TABLES FROM silver;

SELECT region, channel, count(*) AS txns, sum(amount) AS revenue
FROM silver.transactions
GROUP BY region, channel
ORDER BY revenue DESC
LIMIT 10;

-- Iceberg metadata tables work too - snapshots are the ones Snowflake committed
SELECT snapshot_id, committed_at, operation FROM silver."transactions$snapshots";

-- Expected to FAIL: gold.customer_360 has masking + row access policies, Horizon does
-- not vend raw-file credentials for it to an engine that cannot use the Scan Plan API.
SELECT * FROM gold.customer_360 LIMIT 5;
