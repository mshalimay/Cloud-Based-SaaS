# archive_app_config.py
#
# Copyright (C) 2015-2023 Vas Vasiliadis
# University of Chicago
#
# Set app configuration options for archive utility
#
##
__author__ = "Vas Vasiliadis <vas@uchicago.edu>"

import os

base_dir = os.path.abspath(os.path.dirname(__file__))


class Config(object):
    # general settings
    CNETID = "mashalimay"    
    CSRF_ENABLED = True
    AWS_REGION_NAME = "us-east-1"

    # AWS S3 parameters
    AWS_S3_RESULTS_BUCKET = "gas-results"
    KEY_PREFIX = f"{CNETID}/"

    # Set validity of pre-signed POST requests (in seconds)
    AWS_S3_SIGNED_REQUEST_EXPIRATION = 60

    

    # archival 
    ARCHIVALS_DIR = f"{base_dir}/archivals"
    #ARCHIVALS_EXEC_PATH = f"{base_dir}/run.py"

    # AWS DynamoDB table
    AWS_DYNAMODB_ANNOTATIONS_TABLE = f"{CNETID}_annotations"

    # AWS Glacier
    VAULT_NAME = "ucmpcs"

    # SQS
    SQS_ARCHIVAL_REQUESTS_QUEUE_URL = f"https://sqs.{AWS_REGION_NAME}.amazonaws.com/127134666975/{CNETID}_a17_archival_requests"
    AWS_SQS_WAIT_TIME = 20
    AWS_SQS_MAX_MESSAGES = 10
### EOF
