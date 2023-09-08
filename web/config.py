# config.py
#
# Copyright (C) 2015-2023 Vas Vasiliadis
# University of Chicago
#
# Set GAS web app configuration options based on environment
#
# ************************************************************************
#
# >>>>> MAKE CHANGES TO THIS FILE ONLY BELOW LINE 133 <<<<<<<
#
# ************************************************************************
##
__author__ = "Vas Vasiliadis <vas@uchicago.edu>"

import os
import json
import boto3
import base64
from botocore.exceptions import ClientError

base_dir = os.path.abspath(os.path.dirname(__file__))

# Get the IAM username that was stashed at launch time
try:
    with open("/home/ubuntu/.launch_user", "r") as file:
        iam_username = file.read().replace("\n", "")
except FileNotFoundError as e:
    if "LAUNCH_USER" in os.environ:
        iam_username = os.environ["LAUNCH_USER"]
    else:
        # Unable to set username, so exit
        print("Unable to find launch user name in local file or environment!")
        raise e


class Config(object):
    GAS_LOG_LEVEL = (
        os.environ["GAS_LOG_LEVEL"] if ("GAS_LOG_LEVEL" in os.environ) else "INFO"
    )
    GAS_LOG_FILE_PATH = base_dir + (
        os.environ["GAS_LOG_FILE_PATH"]
        if ("GAS_LOG_FILE_PATH" in os.environ)
        else "/log"
    )
    GAS_LOG_FILE_NAME = (
        os.environ["GAS_LOG_FILE_NAME"]
        if ("GAS_LOG_FILE_NAME" in os.environ)
        else "gas.log"
    )

    WSGI_SERVER = "werkzeug"
    CSRF_ENABLED = True

    SSL_CERT_PATH = (
        os.environ["SSL_CERT_PATH"]
        if ("SSL_CERT_PATH" in os.environ)
        else "ssl/ucmpcs.org.crt"
    )
    SSL_KEY_PATH = (
        os.environ["SSL_KEY_PATH"]
        if ("SSL_KEY_PATH" in os.environ)
        else "ssl/ucmpcs.org.key"
    )

    AWS_PROFILE_NAME = (
        os.environ["AWS_PROFILE_NAME"] if ("AWS_PROFILE_NAME" in os.environ) else None
    )
    AWS_REGION_NAME = (
        os.environ["AWS_REGION_NAME"]
        if ("AWS_REGION_NAME" in os.environ)
        else "us-east-1"
    )

    # Get various credentials from AWS Secrets Manager
    asm = boto3.client("secretsmanager", region_name=AWS_REGION_NAME)

    # Get Flask application secret
    try:
        asm_response = asm.get_secret_value(SecretId="gas/web_server")
        flask_secret = json.loads(asm_response["SecretString"])
    except ClientError as e:
        print(f"Unable to retrieve Flask secret from ASM: {e}")
        raise e
    SECRET_KEY = flask_secret["flask_secret_key"]

    # Get RDS secret and construct database URI
    try:
        asm_response = asm.get_secret_value(SecretId="rds/accounts_database")
        rds_secret = json.loads(asm_response["SecretString"])
    except ClientError as e:
        print(f"Unable to retrieve accounts database credentials from ASM: {e}")
        raise e

    if "ACCOUNTS_DATABASE_TABLE" in os.environ:
        SQLALCHEMY_DATABASE_TABLE = os.environ["ACCOUNTS_DATABASE_TABLE"]
    else:
        SQLALCHEMY_DATABASE_TABLE = f"{iam_username}_accounts"

    SQLALCHEMY_DATABASE_URI = (
        "postgresql://"
        + rds_secret["username"]
        + ":"
        + rds_secret["password"]
        + "@"
        + rds_secret["host"]
        + ":"
        + str(rds_secret["port"])
        + "/"
        + SQLALCHEMY_DATABASE_TABLE
    )
    SQLALCHEMY_TRACK_MODIFICATIONS = True

    # Get the Globus Auth client ID and secret
    try:
        asm_response = asm.get_secret_value(SecretId="globus/auth_client")
        globus_auth = json.loads(asm_response["SecretString"])
    except ClientError as e:
        print(f"Unable to retrieve Globus Auth credentials from ASM: {e}")
        raise e

    # Set the Globus Auth client ID and secret
    GAS_CLIENT_ID = globus_auth["gas_client_id"]
    GAS_CLIENT_SECRET = globus_auth["gas_client_secret"]
    GLOBUS_AUTH_LOGOUT_URI = "https://auth.globus.org/v2/web/logout"

    # ************************************************************************
    #
    # DO NOT MODIFY ANY OF THE CODE OR SETTINGS ABOVE
    #
    # ************************************************************************

    # Stripe
    STRIPE_PUBLIC_KEY = "pk_test_51N8sPrAot7JhD84Tehx0d76CtmHaPqGPrxcnYUUFBivfJZc5biOeJL42FPoljn9NAikaChMIssotM1HjmiSIEvWD00cPJCCyzB"
    STRIPE_SECRET_KEY = "sk_test_51N8sPrAot7JhD84TPfQFYvYnxEHnAnjxBK4m0BYIL3Wsb6VXrnVhFLEHEZShkSiNrg05MiP4HSe05FF6BSHFKwyD00Fo2QsFoI"
    STRIPE_PRICE_ID = "price_1N8sSuAot7JhD84TBdIgCRl4"

    # Set validity of pre-signed POST requests (in seconds)
    AWS_SIGNED_REQUEST_EXPIRATION = 60

    # AWS S3 upload parameters
    AWS_S3_INPUTS_BUCKET = "gas-inputs"
    AWS_S3_RESULTS_BUCKET = "gas-results"
    MAX_SIZE_FREE_USER = 150 * 1024

    # Set the S3 key (object name) prefix to your CNetID
    # Keep the trailing '/' if using my upload code in views.py
    AWS_S3_KEY_PREFIX = f"{iam_username}/"

    # S3 delimiter
    AWS_KEY_BASE_DELIMITER = "/"
    AWS_KEY_FINAL_DELIMITER = "~"

    # Signed post parameters

    AWS_S3_ACL = "private"
    AWS_S3_ENCRYPTION = "AES256"

    AWS_GLACIER_VAULT = "ucmpcs"

    # AWS SNS topics
    AWS_SNS_JOB_REQUEST_TOPIC = (
        f"arn:aws:sns:us-east-1:127134666975:{iam_username}_a17_job_requests"
    )
    AWS_SNS_THAW_REQUESTS = (
        f"arn:aws:sns:us-east-1:127134666975:{iam_username}_a17_thaw_requests"
    )
  
    # AWS SQS queues
    AWS_SQS_REQUESTS_QUEUE_NAME = f"{iam_username}_a17_job_requests"

    # AWS DynamoDB table
    AWS_DYNAMODB_ANNOTATIONS_TABLE = f"{iam_username}_annotations"
    DYNAMODB_USER_ID_SEC_INDEX = "user_id_index"

   
    # Use this email address to send email via SES
    MAIL_DEFAULT_SENDER = f"{iam_username}@ucmpcs.org"

    # Time before free user results are archived (in seconds)
    FREE_USER_DATA_RETENTION = 300


class DevelopmentConfig(Config):
    DEBUG = True
    GAS_LOG_LEVEL = "DEBUG"


class ProductionConfig(Config):
    DEBUG = False
    GAS_LOG_LEVEL = "INFO"
    SSL_CERT_PATH = (
        os.environ["SSL_CERT_PATH"]
        if ("SSL_CERT_PATH" in os.environ)
        else "/usr/local/src/ssl/ucmpcs.org.crt"
    )
    SSL_KEY_PATH = (
        os.environ["SSL_KEY_PATH"]
        if ("SSL_KEY_PATH" in os.environ)
        else "/usr/local/src/ssl/ucmpcs.org.key"
    )


### EOF
