# ann_config.py
#
# Copyright (C) 2015-2023 Vas Vasiliadis
# University of Chicago
#
# Set GAS annotator configuration options
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

    # annotator settings
    ANNOTATOR_BASE_DIR = f"{base_dir}"
    ANNOTATOR_JOBS_DIR = f"{base_dir}/jobs"
    ANNTOOLS_EXEC_PATH = f"{base_dir}/run.py"

    # AWS S3 upload parameters
    AWS_S3_INPUTS_BUCKET = "gas-inputs"
    AWS_S3_RESULTS_BUCKET = "gas-results"
    KEY_PREFIX = f"{CNETID}/"

    # AWS SNS topics
    JOB_RESULTS_TOPIC_ARN = f"arn:aws:sns:{AWS_REGION_NAME}:127134666975:{CNETID}_a17_job_results"

    # AWS SQS
    AWS_SQS_WAIT_TIME = 20
    AWS_SQS_MAX_MESSAGES = 10 
    SQS_JOB_REQUESTS_QUEUE_URL = f"https://sqs.{AWS_REGION_NAME}.amazonaws.com/127134666975/{CNETID}_a17_job_requests"

    # AWS DynamoDB
    AWS_DYNAMODB_ANNOTATIONS_TABLE = f"{CNETID}_annotations"


### EOF
