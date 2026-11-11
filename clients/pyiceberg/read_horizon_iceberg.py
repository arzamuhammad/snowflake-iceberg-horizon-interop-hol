"""
Lab 6a - Read Snowflake-managed Iceberg tables through the Horizon Iceberg REST Catalog
with PyIceberg (laptop, SageMaker notebook, Lambda, any Python runtime).

    pip install -r requirements.txt
    export SNOWFLAKE_ACCOUNT=<org>-<account>        # e.g. myorg-myaccount
    export SNOWFLAKE_PAT=<PAT of SVC_GROUP_DBX_READER>
    python read_horizon_iceberg.py

What this lab shows (verified with PyIceberg 0.12):
  * RBAC is enforced by Horizon: only namespaces/tables granted to the role are listed.
  * Unprotected tables (SILVER.TRANSACTIONS) are read with short-lived S3 credentials
    vended by Horizon - the engine needs no AWS keys of its own.
  * Policy-protected tables (masking / row access) are NOT handed out as raw files:
    Horizon refuses to vend credentials, so the read fails with ACCESS_DENIED.
    Enforced reads need an engine that implements the asynchronous Scan Plan API
    (Spark / Databricks with Iceberg 1.11+, see Lab 6b). PyIceberg 0.12 does not yet.

Run this WITHOUT personal AWS credentials in the environment. If your shell has an
AWS profile that can read the bucket directly, PyIceberg silently falls back to it and
reads the raw files - that is an IAM hole, not a Horizon one (see README, "Lessons").
"""
import os

from pyiceberg.catalog.rest import RestCatalog

ACCOUNT = os.environ["SNOWFLAKE_ACCOUNT"]
PAT = os.environ["SNOWFLAKE_PAT"]
ROLE = os.getenv("SNOWFLAKE_ROLE", "GROUP_DBX_READER")
DATABASE = os.getenv("SNOWFLAKE_DATABASE", "GROUP_LAKEHOUSE")  # = Iceberg "warehouse", UPPER CASE

catalog = RestCatalog(
    name="horizon",
    **{
        "uri": f"https://{ACCOUNT}.snowflakecomputing.com/polaris/api/catalog",
        "warehouse": DATABASE,
        "credential": PAT,                       # PAT is exchanged for a short-lived token
        "scope": f"session:role:{ROLE}",
        "header.X-Iceberg-Access-Delegation": "vended-credentials",
    },
)

print("Namespaces visible to", ROLE, "->", catalog.list_namespaces())
for ns in catalog.list_namespaces():
    print("  ", ns[0], "->", [t[1] for t in catalog.list_tables(ns)])

# Unprotected Silver table -> works with vended credentials
txn = catalog.load_table("SILVER.TRANSACTIONS")
print("\nSILVER.TRANSACTIONS schema:\n", txn.schema())
print(txn.scan(selected_fields=("TXN_ID", "REGION", "AMOUNT"), limit=5).to_pandas())

# Policy-protected Gold table -> Horizon does not vend raw-file credentials
try:
    catalog.load_table("GOLD.CUSTOMER_360").scan(limit=5).to_pandas()
    print("\nWARNING: protected table was readable - check for AWS credentials in your environment")
except OSError as exc:
    print("\nGOLD.CUSTOMER_360 -> blocked as expected (protected table, no vended credentials)")
    print("  ", str(exc)[:160])
