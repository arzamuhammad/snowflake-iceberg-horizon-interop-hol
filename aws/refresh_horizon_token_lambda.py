"""
Lambda - keep the Glue federation bearer token fresh.

The Horizon access token obtained from a PAT is valid for ~1 hour. The Glue
connection reads it from Secrets Manager (key BEARER_TOKEN), so refresh it on a
schedule (EventBridge rate(50 minutes)).

Environment variables:
  SNOWFLAKE_ACCOUNT   <org>-<account>
  SNOWFLAKE_ROLE      GROUP_AWS_READER
  PAT_SECRET_ID       group-lakehouse/horizon-pat     ({"PAT": "<token_secret>"})
  TOKEN_SECRET_ID     group-lakehouse/horizon-token   ({"BEARER_TOKEN": "..."})
IAM: secretsmanager:GetSecretValue on PAT_SECRET_ID, secretsmanager:PutSecretValue on TOKEN_SECRET_ID.
"""
import json
import os
import urllib.parse
import urllib.request

import boto3

sm = boto3.client("secretsmanager")


def handler(event, context):
    pat = json.loads(sm.get_secret_value(SecretId=os.environ["PAT_SECRET_ID"])["SecretString"])["PAT"]
    body = urllib.parse.urlencode({
        "grant_type": "client_credentials",
        "scope": f"session:role:{os.environ['SNOWFLAKE_ROLE']}",
        "client_secret": pat,
    }).encode()
    req = urllib.request.Request(
        f"https://{os.environ['SNOWFLAKE_ACCOUNT']}.snowflakecomputing.com/polaris/api/catalog/v1/oauth/tokens",
        data=body,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        token = json.loads(resp.read())["access_token"]
    sm.put_secret_value(SecretId=os.environ["TOKEN_SECRET_ID"],
                        SecretString=json.dumps({"BEARER_TOKEN": token}))
    return {"status": "refreshed"}
