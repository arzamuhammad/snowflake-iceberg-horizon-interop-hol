/* =============================================================================
   Lab 4 - Service users + Programmatic Access Tokens (PAT) for external engines
   Run as: ACCOUNTADMIN
   -----------------------------------------------------------------------------
   One SERVICE user per consuming company, each with exactly one read-only role.
   The PAT is the secret the engine sends to the Horizon IRC token endpoint;
   the engine asks for a role through the OAuth scope  session:role:<ROLE>.
   Alternatives to PAT: key-pair JWT, External OAuth, Workload Identity Federation.
   ========================================================================== */
USE ROLE ACCOUNTADMIN;

CREATE USER IF NOT EXISTS SVC_GROUP_DBX_READER
  TYPE = SERVICE DEFAULT_ROLE = GROUP_DBX_READER
  COMMENT = 'Databricks / Spark / Trino / PyIceberg access through Horizon IRC';
GRANT ROLE GROUP_DBX_READER TO USER SVC_GROUP_DBX_READER;

CREATE USER IF NOT EXISTS SVC_GROUP_AWS_READER
  TYPE = SERVICE DEFAULT_ROLE = GROUP_AWS_READER
  COMMENT = 'AWS Glue catalog federation (Athena) through Horizon IRC';
GRANT ROLE GROUP_AWS_READER TO USER SVC_GROUP_AWS_READER;

-- PATs for SERVICE users require a network policy by default.
-- Production: attach a network policy that allows only the engines' egress IPs
-- (Databricks NAT, EMR/Glue VPC NAT, Trino hosts), or use Workload Identity
-- Federation / External OAuth instead of PATs.
-- Lab only: relax that requirement for these two users with an authentication policy.
CREATE AUTHENTICATION POLICY IF NOT EXISTS GROUP_LAKEHOUSE.GOVERNANCE.HORIZON_IRC_PAT_POLICY
  AUTHENTICATION_METHODS = ('PROGRAMMATIC_ACCESS_TOKEN')
  PAT_POLICY = (NETWORK_POLICY_EVALUATION = ENFORCED_NOT_REQUIRED, MAX_EXPIRY_IN_DAYS = 90)
  COMMENT = 'LAB ONLY - PAT without network policy for Horizon IRC service users';
ALTER USER SVC_GROUP_DBX_READER SET AUTHENTICATION POLICY GROUP_LAKEHOUSE.GOVERNANCE.HORIZON_IRC_PAT_POLICY;
ALTER USER SVC_GROUP_AWS_READER SET AUTHENTICATION POLICY GROUP_LAKEHOUSE.GOVERNANCE.HORIZON_IRC_PAT_POLICY;

-- Generate the tokens. COPY the "token_secret" value of each result immediately -
-- Snowflake shows it only once. Store it in your secret manager
-- (Databricks secret scope, AWS Secrets Manager, ...), never in code.
ALTER USER SVC_GROUP_DBX_READER ADD PROGRAMMATIC ACCESS TOKEN HOL_DBX_PAT
  DAYS_TO_EXPIRY = 30 ROLE_RESTRICTION = 'GROUP_DBX_READER'
  COMMENT = 'Horizon IRC - Databricks/Spark/Trino';

ALTER USER SVC_GROUP_AWS_READER ADD PROGRAMMATIC ACCESS TOKEN HOL_AWS_PAT
  DAYS_TO_EXPIRY = 30 ROLE_RESTRICTION = 'GROUP_AWS_READER'
  COMMENT = 'Horizon IRC - AWS Glue federation';

SHOW USER PROGRAMMATIC ACCESS TOKENS FOR USER SVC_GROUP_DBX_READER;

-- Rotate / revoke later with:
--   ALTER USER SVC_GROUP_DBX_READER ROTATE PROGRAMMATIC ACCESS TOKEN HOL_DBX_PAT;
--   ALTER USER SVC_GROUP_DBX_READER REMOVE PROGRAMMATIC ACCESS TOKEN HOL_DBX_PAT;
