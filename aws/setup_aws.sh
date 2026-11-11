#!/usr/bin/env bash
# Lab 1 (AWS side) - S3 bucket + IAM role that Snowflake assumes for the external volume.
# Requires: AWS CLI v2 with rights to create S3 buckets and IAM roles.
#
#   export BUCKET=<globally-unique-bucket>  AWS_REGION=ap-southeast-3
#   ./aws/setup_aws.sh create            # before sql/01_external_volume.sql
#   ./aws/setup_aws.sh trust <STORAGE_AWS_IAM_USER_ARN> <STORAGE_AWS_EXTERNAL_ID>
#                                         # after DESC EXTERNAL VOLUME
set -euo pipefail
cd "$(dirname "$0")"

: "${BUCKET:?set BUCKET}"
: "${AWS_REGION:?set AWS_REGION}"
ROLE_NAME="${ROLE_NAME:-snowflake-lakehouse-role}"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

render() { sed -e "s/<your-bucket>/${BUCKET}/g" -e "s/<aws-account-id>/${ACCOUNT_ID}/g" "$1"; }

case "${1:-}" in
  create)
    if [ "$AWS_REGION" = "us-east-1" ]; then
      aws s3api create-bucket --bucket "$BUCKET"
    else
      aws s3api create-bucket --bucket "$BUCKET" --region "$AWS_REGION" \
        --create-bucket-configuration LocationConstraint="$AWS_REGION"
    fi
    aws s3api put-public-access-block --bucket "$BUCKET" --public-access-block-configuration \
      BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
    aws s3api put-bucket-versioning --bucket "$BUCKET" --versioning-configuration Status=Enabled

    # Placeholder trust (own account) - replaced by the Snowflake principal in step "trust"
    sed -e "s#<STORAGE_AWS_IAM_USER_ARN>#arn:aws:iam::${ACCOUNT_ID}:root#" \
        -e "s#<STORAGE_AWS_EXTERNAL_ID>#group_lakehouse_ext_id#" trust-policy.json > /tmp/trust.json
    aws iam create-role --role-name "$ROLE_NAME" --assume-role-policy-document file:///tmp/trust.json \
      --description "Snowflake external volume for GROUP_LAKEHOUSE" >/dev/null
    render s3-access-policy.json > /tmp/s3.json
    aws iam put-role-policy --role-name "$ROLE_NAME" --policy-name snowflake-lakehouse-s3 \
      --policy-document file:///tmp/s3.json
    echo "Role ARN: arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}"
    echo "Next: run sql/01_external_volume.sql, then: $0 trust <IAM_USER_ARN> <EXTERNAL_ID>"
    ;;
  trust)
    : "${2:?STORAGE_AWS_IAM_USER_ARN}"; : "${3:?STORAGE_AWS_EXTERNAL_ID}"
    sed -e "s#<STORAGE_AWS_IAM_USER_ARN>#$2#" -e "s#<STORAGE_AWS_EXTERNAL_ID>#$3#" \
      trust-policy.json > /tmp/trust.json
    aws iam update-assume-role-policy --role-name "$ROLE_NAME" --policy-document file:///tmp/trust.json
    echo "Trust updated. Wait ~30 s, then run SELECT SYSTEM\$VERIFY_EXTERNAL_VOLUME('LAKEHOUSE_EXT_VOL');"
    ;;
  *) echo "usage: $0 create | trust <IAM_USER_ARN> <EXTERNAL_ID>"; exit 1 ;;
esac
