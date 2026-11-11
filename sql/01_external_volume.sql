/* =============================================================================
   Lab 1 - External volume on the customer's own Amazon S3 bucket
   Run as: ACCOUNTADMIN
   Prerequisite: aws/setup_aws.sh (creates bucket + IAM role) - see README
   ========================================================================== */
USE ROLE ACCOUNTADMIN;

-- Replace the 3 placeholders: <your-bucket>, <aws-account-id>, and keep the external id
-- identical to the one in aws/trust-policy.json
CREATE EXTERNAL VOLUME IF NOT EXISTS LAKEHOUSE_EXT_VOL
  STORAGE_LOCATIONS = ((
    NAME                 = 'lakehouse-s3'
    STORAGE_PROVIDER     = 'S3'
    STORAGE_BASE_URL     = 's3://<your-bucket>/lakehouse/'
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<aws-account-id>:role/snowflake-lakehouse-role'
    STORAGE_AWS_EXTERNAL_ID = 'group_lakehouse_ext_id'
    -- Optional SSE-KMS: ENCRYPTION = (TYPE = 'AWS_SSE_KMS' KMS_KEY_ID = '<full-kms-key-arn>')
  ))
  ALLOW_WRITES = TRUE;

-- Copy STORAGE_AWS_IAM_USER_ARN from the output into aws/trust-policy.json,
-- then re-run `aws iam update-assume-role-policy` (step in setup_aws.sh)
DESC EXTERNAL VOLUME LAKEHOUSE_EXT_VOL;

-- Must return "success": true (read / write / list / delete all PASSED)
SELECT SYSTEM$VERIFY_EXTERNAL_VOLUME('LAKEHOUSE_EXT_VOL');

-- The table OWNER must keep USAGE on the volume, otherwise every request for
-- vended credentials from external engines fails.
GRANT USAGE ON EXTERNAL VOLUME LAKEHOUSE_EXT_VOL TO ROLE LAKEHOUSE_ADMIN;
GRANT USAGE ON EXTERNAL VOLUME LAKEHOUSE_EXT_VOL TO ROLE DATA_ENGINEER;
-- Roles that read POLICY-PROTECTED tables through the Scan Plan API also need USAGE:
-- Horizon writes the filtered (masked) dataset to this volume for them.
GRANT USAGE ON EXTERNAL VOLUME LAKEHOUSE_EXT_VOL TO ROLE GROUP_DBX_READER;
GRANT USAGE ON EXTERNAL VOLUME LAKEHOUSE_EXT_VOL TO ROLE GROUP_AWS_READER;
