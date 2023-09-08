# views.py
#
# Copyright (C) 2015-2023 Vas Vasiliadis
# University of Chicago
#
# Application logic for the GAS
#
##

__author__ = "Vas Vasiliadis <vas@uchicago.edu>"

import uuid
import time
import json

import boto3
from botocore.client import Config
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr

from flask import abort, flash, redirect, render_template, request, session, url_for
import stripe

import requests
from app import app, db
from decorators import authenticated, is_premium

import os

#-------------------------------------------------------------------
## website endpoints
#-------------------------------------------------------------------
"""Start annotation request
Create the required AWS S3 policy document and render a form for
uploading an annotation input file using the policy document
"""

@app.route("/annotate", methods=["GET"])
@authenticated
def annotate():
    # Open a connection to the S3 service
    s3 = boto3.client(
        "s3",
        region_name=app.config["AWS_REGION_NAME"],
        config=Config(signature_version="s3v4"),
    )

    bucket_name = app.config["AWS_S3_INPUTS_BUCKET"]
    user_id = session["primary_identity"]

    # Generate unique ID to be used as S3 key (name)
    key_name = (
        app.config["AWS_S3_KEY_PREFIX"]
        + user_id
        + "/"
        + str(uuid.uuid4())
        + app.config["AWS_KEY_FINAL_DELIMITER"] + "${filename}"
    )

    # Create the redirect URL
    redirect_url = str(request.url) + "/job"

    # Define policy conditions
    encryption = app.config["AWS_S3_ENCRYPTION"]
    acl = app.config["AWS_S3_ACL"]
    fields = {
        "success_action_redirect": redirect_url,
        "x-amz-server-side-encryption": encryption,
        "acl": acl,
        "csrf_token": app.config["SECRET_KEY"]
    }
    conditions = [
        ["starts-with", "$success_action_redirect", redirect_url],
        {"x-amz-server-side-encryption": encryption},
        {"acl": acl},
        ["starts-with", "$csrf_token", ""]
    ]

    # Generate the presigned POST call
    try:
        presigned_post = s3.generate_presigned_post(
            Bucket=bucket_name,
            Key=key_name,
            Fields=fields,
            Conditions=conditions,
            ExpiresIn=app.config["AWS_SIGNED_REQUEST_EXPIRATION"],
        )
    except ClientError as e:
        app.logger.error(f"Unable to generate presigned URL for upload: {e}")
        return abort(500)

    # Render the upload form which will parse/submit the presigned POST
    return render_template("annotate.html", s3_post=presigned_post, 
                           user_role=session["role"], max_file_size=app.config["MAX_SIZE_FREE_USER"])


"""Fires off an annotation job
Accepts the S3 redirect GET request, parses it to extract 
required info, saves a job item to the database, and then
publishes a notification for the annotator service.
"""
@app.route("/annotate/job", methods=["GET"])
@authenticated
def create_annotation_job_request():
    aws_region = app.config["AWS_REGION_NAME"]
    dynamo_tbl_name = app.config["AWS_DYNAMODB_ANNOTATIONS_TABLE"]
    user_id = session["primary_identity"]
    key_delimiter = app.config["AWS_KEY_FINAL_DELIMITER"]

    #---------------------------------------------------------------------------
    # Get bucket name, key from the S3 redirect URL
    #---------------------------------------------------------------------------
    try:
        s3_inputs_bucket = request.args['bucket']   # eg: gas-inputs
        s3_key = request.args['key']                # eg: mashalimay/userX/<file_uuid>~test.vcf
    except KeyError as e:
        delete_s3_file(s3_inputs_bucket, s3_key)
        message = f"Missing inputs from /annotate POST redirect. Error message: {str(e)}"
        print(message)
        app.logger.error(message)
        return abort(500)
    except Exception as e:
        delete_s3_file(s3_inputs_bucket, s3_key)
        message = f"Unexpected error while retieving inputs from /annotate POST redirect. Error message: {str(e)}"
        app.logger.error(message)
        return abort(500)

    # Parse the S3 key to get the job ID, input file name
    # obs(!): string manipulations assume s3 key will be separated by '/'
    job_id = os.path.basename(s3_key).split(key_delimiter)[0]            # eg: <file_uuid>
    input_file_name = os.path.basename(s3_key).split(key_delimiter)[1]   # eg: test.vcf

    #---------------------------------------------------------------------------
    # create item to persist in DynamoDB database
    #---------------------------------------------------------------------------
    # obs(!): DynamoDB keys and attributes are not in config.py following Professor instructions

    dynamodb = boto3.client('dynamodb', config=Config(signature_version='s3v4'), region_name=aws_region)
    
    # @ref AWS docs for PUT using boto3.client: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/put_item.html
    # @ref Another example: https://binaryguy.tech/aws/dynamodb/put-items-into-dynamodb-table-using-python/ 
    
    # Obs about below code:
    # (i) int(time()) to follow Professor example
    # (ii) when using boto3.client, all values must be passed as strings, including the 'N' types
     
    data = {
        'job_id': {'S': job_id},
        'user_id': {'S': user_id},
        'input_file_name': {'S': input_file_name},
        's3_inputs_bucket': {'S': s3_inputs_bucket},
        's3_key_input_file': {'S': s3_key},
        'submit_time': {'N': str(int(time.time()))}, 
        'job_status': {'S': 'PENDING'}
    }
    
    # try to persist the job item to DynamoDB
    try:
        response = dynamodb.put_item(
            TableName = dynamo_tbl_name,
            Item = data
        )        
    except ClientError as e:
        # Try to delete the file from S3 to not have an orphan file in the S3 bucket
        delete_s3_file(s3_inputs_bucket, s3_key)
        message = (f"Annotation job not executed. Failed to save job data to internal database." +
                       f"AWS Error message: {str(e)}. AWS HTTP status code: {e.response['ResponseMetadata']['HTTPStatusCode']}")
        
        app.logger.error(message)       
        return abort(500)
            
    #---------------------------------------------------------------------------
    # publish job request to the queue
    #---------------------------------------------------------------------------
    # @ref syntax and exceptions for publishing a message to AWS SNS topic: 
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns/client/publish.html

    # @ref guide to working with SNS using boto3.client; specific example of pub message 
    # to SNS topic in the end of the post: https://towardsdatascience.com/working-with-amazon-sns-with-boto3-7acb1347622d

    try:
        sns_client = boto3.client('sns', config=Config(signature_version='s3v4'), region_name=aws_region)

        # Publish the message to the SNS topic
        response = sns_client.publish(
            TopicArn = app.config["AWS_SNS_JOB_REQUEST_TOPIC"],
            Message = json.dumps({
                'job_id': job_id,
                'user_id': user_id,
                'input_file_name': input_file_name,
                's3_inputs_bucket': s3_inputs_bucket,
                's3_key_input_file': s3_key,
                'prefix': app.config["AWS_S3_KEY_PREFIX"],
                'email': session['email'],
                'role': session['role']         # might be useful in the future
                }
            )
        )
        
    except ClientError as e:
        # Try to delete entries from S3 and DynamoDB to not have orphan data
        delete_s3_file(s3_inputs_bucket, s3_key)
        delete_dynamo_key(db_client=dynamodb, table_name=dynamo_tbl_name,
                           key_name='job_id', value=job_id)

        # if no specific error code caught, provide information that can be helpful
        # following guidelines in https://boto3.amazonaws.com/v1/documentation/api/latest/guide/error-handling.html
        app.logger.error(f"Unable to submit annotation job due to an error with SNS AWS service. " +
                        f"AWS Error message: {e.response['Error']['Message']}. " +
                        f"AWS Request ID: {e.response['ResponseMetadata']['RequestId']}. " +
                        f"AWS HTTP Status Code: {e.response['ResponseMetadata']['HTTPStatusCode']}")
        return abort(500)

    except Exception as e:
        # Try to delete entries from S3 and DynamoDB to not have orphan data
        delete_s3_file(s3_inputs_bucket, s3_key)
        delete_dynamo_key(db_client=dynamodb, table_name=dynamo_tbl_name, 
                          key_name='job_id', value=job_id)
        
        app.logger.error(f"Failed to submit the job to the annotation server due to unexpected error. "+
                       f"Error message: {str(e)}. Error type: {type(e).__name__}")
        return abort(500)

    return render_template("annotate_confirm.html", job_id=job_id)


"""List all annotations for the user
"""
@app.route("/annotations", methods=["GET"])
@authenticated
def annotations_list():
    # Create a DynamoDB client
    dynamodb = boto3.resource('dynamodb', config=Config(signature_version='s3v4'), 
                              region_name = app.config["AWS_REGION_NAME"])

    # Try to get table resource and query for list of jobs
    # @ref to create table resource and query using partition keys:
    # see example #4 in https://www.fernandomc.com/posts/ten-examples-of-getting-data-from-dynamodb-with-python-and-boto3/
    # @ref another good reference with examples: https://binaryguy.tech/aws/dynamodb/query-data-from-dynamodb-table-with-python/
    # @ref AWS docs for the syntax and return structure https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/query.html#
    try:
        table = dynamodb.Table(app.config["AWS_DYNAMODB_ANNOTATIONS_TABLE"])

        # Query the table using the secondary index, associated to the user_id
        response = table.query(
            IndexName=app.config['DYNAMODB_USER_ID_SEC_INDEX'],
            KeyConditionExpression=Key("user_id").eq(session["primary_identity"]))
            
        # retrieve desired attributes from DynamoDB
        annotations = []
        for item in response['Items']:
            job_id = item["job_id"]
            submit_time = local_time(int(item["submit_time"]))
            input_file_name = item["input_file_name"]
            job_status = item["job_status"]
            
            annotations.append([job_id, submit_time, input_file_name, job_status])

    except ClientError as e:
        app.logger.error(f"ClientError while retrieving list of jobs from DynamoDB." +
                       f"AWS Error message: {e.response['Error']['Message']}. " +
                       f"AWS Request ID: {e.response['ResponseMetadata']['RequestId']}. " +
                       f"AWS HTTP Status Code: {e.response['ResponseMetadata']['HTTPStatusCode']}")
        return abort(500)

    except Exception as e:
        app.logger.error(f"Failed to retrieve list of jobs from DynamoDB due to unexpected error."+
                       f"Error message: {str(e)}. Error type: {type(e).__name__}")
        return abort(500)

    return render_template('annotations.html', annotations=annotations)


"""Display details of a specific annotation job
"""
@app.route("/annotations/<id>", methods=["GET"])
@authenticated
def annotation_details(id):

    #---------------------------------------------------------------------------
    # retrieve job data from DynamoDB
    #---------------------------------------------------------------------------
    # Create a DynamoDB client
    dynamodb = boto3.resource('dynamodb', config=Config(signature_version='s3v4'), 
                              region_name = app.config["AWS_REGION_NAME"])

    # Try to get table resource and query for list of jobs
    # @ref to create table resource and "get_item":
    # see example #2 in https://www.fernandomc.com/posts/ten-examples-of-getting-data-from-dynamodb-with-python-and-boto3/
    # @ref another good reference with examples: https://binaryguy.tech/aws/dynamodb/query-data-from-dynamodb-table-with-python/
    try:
        table = dynamodb.Table(app.config["AWS_DYNAMODB_ANNOTATIONS_TABLE"])

        # Query the table using the job_id
        response = table.get_item(
        Key={
              "job_id": id
            }
        )
        item = response['Item']

    except ClientError as e:
        app.logger.error(f"ClientError while retrieving job data from DynamoDB." +
                       f"AWS Error message: {e.response['Error']['Message']}. " +
                       f"AWS Request ID: {e.response['ResponseMetadata']['RequestId']}. " +
                       f"AWS HTTP Status Code: {e.response['ResponseMetadata']['HTTPStatusCode']}")
        return abort(500)

    except KeyError as e:
        app.logger.error(f"KeyError while retrieving job data from DynamoDB. Probably invalid job id."+
                       f"Error message: {str(e)}")
        return abort(404)

    except Exception as e:
        app.logger.error(f"Unexpected error while retrieving job from DynamoDB."+
                       f"Error message: {str(e)}. Error type: {type(e).__name__}")
        return abort(500)
        
    #---------------------------------------------------------------------------
    # Parse job data and render details page 
    #---------------------------------------------------------------------------
    # check if the user is authorized to view this job
    if item['user_id'] != session["primary_identity"]:
        return abort(403)
    
    # retrieve desired entries from DynamoDB table
    annotations = {}
    
    # Obs: details page is not rendered if ANY of the presigned URL that should be generated fails
    # retrieve 'input_file' from DynamoDB & generate presigned URL to input file
    try:
        url = gen_presigned_url_s3_download(app.config["AWS_S3_INPUTS_BUCKET"], item['s3_key_input_file'])
        annotations['input_file'] = [item['input_file_name'], url]

    except ClientError as e:
            app.logger.error(f"ClientError while generating presigned URL for S3 download of input file." +
                        f"AWS Error message: {e.response['Error']['Message']}. " +
                        f"AWS Request ID: {e.response['ResponseMetadata']['RequestId']}. " +
                        f"AWS HTTP Status Code: {e.response['ResponseMetadata']['HTTPStatusCode']}")
            return abort(500)
    except KeyError as e:
        app.logger.error(f"KeyError while accessing 'input_file_name' from DynamoDB. Please check the Database is correct."+
                          f"Error message: {str(e)}.")
        return abort(500)
    except Exception as e:
        app.logger.error(f"Unexpected error while generating presigned URL for S3 download of input file."+
                          f"Error message: {str(e)}. Error type: {type(e).__name__}")
        return abort(500)

    # retrieve result_file from DynamoDB
    # if exists, try generate presigned URL to input file; if error, abort 500    
    if "s3_key_result_file" in item:
        annotations['result_file'] = True
        annotations['result_file_link'] = "link_to_file"

        # if is free user and had his data archived, show link to upgrade to premium
        if session['role']=="free_user" and "results_file_archive_id" in item:            
            annotations['result_file_link'] = "make_me_premium"
            annotations['complete_time'] = local_time(int(item["complete_time"]),format="@")

        elif session['role']=="premium_user" and "results_file_archive_id" in item:
            annotations['result_file_link'] = "file_being_restored"
            
        # otherwise, show link to donwload results
        else:     
            try:
                url = gen_presigned_url_s3_download(app.config["AWS_S3_RESULTS_BUCKET"], item["s3_key_result_file"])
                annotations['result_file'] = url
                annotations['complete_time'] = local_time(int(item["complete_time"]),format="@")
                annotations['log_file'] = "view"

            except ClientError as e:
                app.logger.error(f"ClientError while generating presigned URL for S3 download of results file." +
                            f"AWS Error message: {e.response['Error']['Message']}. " +
                            f"AWS Request ID: {e.response['ResponseMetadata']['RequestId']}. " +
                            f"AWS HTTP Status Code: {e.response['ResponseMetadata']['HTTPStatusCode']}")
                return abort(500)

            except KeyError as e:
                app.logger.error(f"KeyError while accessing elements from DynamoDB. Please check the Database is correct."+
                            f"Error message: {str(e)}.")
                return abort(500)

            except Exception as e:
                app.logger.error(f"Unexpected error while generating presigned URL for S3 download of result file."+
                            f"Error message: {str(e)}. Error type: {type(e).__name__}")
                return abort(500)
            
    else:
        # if there is no result file, set result file, log file and complete time to none
        # obs: this  forces return of log and complete time only if there is a result file; 
        #      in line with the expected behavior for the app
        annotations['result_file'] = None
        annotations['log_file'] = None
        annotations['complete_time'] = None

    # retrieve request_id, status and request_time from DynamoDB
    try:
        annotations['request_id'] = item["job_id"]
        annotations['status'] = item["job_status"]
        annotations['request_time'] = local_time(int(item["submit_time"]), format="@")
    except KeyError as e:
            app.logger.error(f"KeyError while accessing elements from DynamoDB. Please check the Database is correct."+
                          f"Error message: {str(e)}.")
            return abort(500)

    return render_template("annotation.html", annotations=annotations)


"""Display the log file contents for an annotation job
"""


@app.route("/annotations/<id>/log", methods=["GET"])
@authenticated
def annotation_log(id):

    #---------------------------------------------------------------------------
    # Retrieve data for log file from DynamoDB
    #---------------------------------------------------------------------------
    # Create a DynamoDB client
    dynamodb = boto3.resource('dynamodb', config=Config(signature_version='s3v4') 
                              ,region_name = app.config["AWS_REGION_NAME"])
    
    # try to get s3_key_log_file from DynamoDB using the provided id
    try:
        table = dynamodb.Table(app.config["AWS_DYNAMODB_ANNOTATIONS_TABLE"])

        # Query the table using the job_id
        response = table.get_item(
        Key={
              "job_id": id
            }
        )
        item = response['Item']
    
    except ClientError as e:
        app.logger.error(f"ClientError while retrieving log file from DynamoDB." +
                       f"AWS Error message: {e.response['Error']['Message']}. " +
                       f"AWS Request ID: {e.response['ResponseMetadata']['RequestId']}. " +
                       f"AWS HTTP Status Code: {e.response['ResponseMetadata']['HTTPStatusCode']}")
        return abort(500)
    
    except KeyError as e:
        app.logger.error(f"KeyError while retrieving log file from DynamoDB. Probably invalid job id."+
                       f"Error message: {str(e)}")
        return abort(404)

    except Exception as e:
        app.logger.error(f"Unexpected error while retrieving log file from DynamoDB."+
                       f"Error message: {str(e)}. Error type: {type(e).__name__}")
        return abort(500)

    #---------------------------------------------------------------------------
    # Validate data and retrieve log file from S3
    #---------------------------------------------------------------------------

    # check if there is a log file for the job
    if "s3_key_log_file" not in item:
        return abort(404)

    # check if the user is authorized to view this log
    if item['user_id'] != session["primary_identity"]:
        return abort(403)

    # instantiate S3 client
    s3 = boto3.client('s3', config=Config(signature_version="s3v4"), 
                      region_name = app.config["AWS_REGION_NAME"])

    # try to retrieve log file from S3
    try:
        s3_key_log_file = item['s3_key_log_file']
        log_file_object = s3.get_object(Bucket=app.config["AWS_S3_RESULTS_BUCKET"], Key=s3_key_log_file)
        log_file_content = log_file_object['Body'].read().decode('utf-8')

    except ClientError as e:
        app.logger.error(f"ClientError while retrieving log file from S3." +
                       f"AWS Error message: {e.response['Error']['Message']}. " +
                       f"AWS Request ID: {e.response['ResponseMetadata']['RequestId']}. " +
                       f"AWS HTTP Status Code: {e.response['ResponseMetadata']['HTTPStatusCode']}")
        return abort(500)

    except KeyError as e:
        app.logger.error(f"KeyError while accessing trying to retrive log file from S3. DynamoDB is possibly incorrect."+
                       f"Error message: {str(e)}")
        return abort(500)

    except Exception as e:
        app.logger.error(f"Failed to retrieve log file from S3 due to unexpected error."+
                       f"Error message: {str(e)}. Error type: {type(e).__name__}")
        return abort(500)

    return render_template("view_log.html", log_file_content=log_file_content)


"""Subscription management handler
"""
import stripe
from auth import update_profile


@app.route("/subscribe", methods=["GET", "POST"])
@authenticated
def subscribe():
    if request.method == "GET":
        # Display form to get subscriber credit card info
        return render_template("subscribe.html", user_role = session['role'])

    elif request.method == "POST":
        #-----------------------------------------------------------------------
        # Subscribe user to Stripe
        #-----------------------------------------------------------------------
        try:
            stripe.api_key = app.config['STRIPE_SECRET_KEY']

            # Extract the Stripe token from the submitted form
            print("Extracting token from form")
            token = request.form.get('stripe_token')

            # Extract user data from session
            email = session['email']
            username = session['name']

            # Create a customer on Stripe
            # @ref https://stripe.com/docs/api/customers/create
            print("Creating customer on Stripe")
            customer = stripe.Customer.create(
                source=token,
                email=email,
                name=username
            )       

            # Subscribe customer to pricing plan
            # @ref https://stripe.com/docs/api/subscriptions/create
            print("Subscribing customer to pricing plan")
            subscription = stripe.Subscription.create(
                customer=customer.id,
                items = [
                    {"price": app.config['STRIPE_PRICE_ID']}
                ]
            )

            if subscription.status != "active":
                message = f"Unable to subscribe customer to pricing plan." \
                    + f"Subscription status: {subscription.status}." \
                    + f"Will try to delete subscription"                
                print(message)
                app.logger.error(message)
                cancel_strip_subscription(subscription)
                return abort(500)

        #@ref stripe error handling: https://stripe.com/docs/error-handling?lang=python#catch-exceptions    
        except stripe.error.StripeError as e:
            message = f"StripeError while processing subscription request. Error message: {str(e)}."
            app.logger.error(message)
            print(message)
            if subscription:
                cancel_strip_subscription(subscription)
            return abort(500)

        except Exception as e:
            message = (f"Unexpected error while processing subscription request."+
            f"Error message: {str(e)}. Error type: {type(e).__name__}")
            app.logger.error(message)
            print(message)
            if subscription:
                cancel_strip_subscription(subscription)
            return abort(500)
        #-----------------------------------------------------------------------
        # update user role in database and session
        #-----------------------------------------------------------------------
        try:
            print("Updating customer profile in database")
            update_profile(identity_id=session["primary_identity"], role="premium_user")
            # Update role in the session
            print("update customer role in Flask session")
            session['role'] = 'premium_user'

        except ClientError as e:
            message = ("ClientError while updating customer profile in database."+
                  "Stripe subscription will try to be rolled back." +
                  f"AWS Error message: {e.response['Error']['Message']}. " +
                  f"AWS Request ID: {e.response['ResponseMetadata']['RequestId']}. " +
                  f"AWS HTTP Status Code: {e.response['ResponseMetadata']['HTTPStatusCode']}")

            print(message)
            app.logger.error(message)
            cancel_strip_subscription(subscription)
            
            return abort(500)
        except Exception as e:
            message = (f"Unexpected error while updating customer profile in database."+
                    "Stripe subscription will try to be rolled back." +
                    f"Error message: {str(e)}. Error type: {type(e).__name__}")
            print(message)
            app.logger.error(message)
            cancel_strip_subscription(subscription)
            return abort(500)
        
        #-----------------------------------------------------------------------
        # Request restoration of the user's data from Glacier
        #-----------------------------------------------------------------------
        print("Ask restoration of user data from Glacier")
        # try to query DynamoDB for user archivals
        try:
            user_id = session["primary_identity"]
            tbl_name = app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE']
            secondary_index = app.config['DYNAMODB_USER_ID_SEC_INDEX']
            glacier_key = "results_file_archive_id"
            
            table = boto3.resource('dynamodb', config=Config(signature_version='s3v4'), 
                                   region_name=app.config["AWS_REGION_NAME"]).Table(tbl_name)

            response = table.query(
                IndexName=secondary_index,
                KeyConditionExpression=Key('user_id').eq(user_id),
                FilterExpression=Attr(glacier_key).exists(),
                # gets only desired fields for faster retrieval; do not need s3 data if querying Dynamo in lambda fun
                ProjectionExpression=f"{glacier_key}, job_id" # s3_results_bucket, s3_key_result_file"
            )
            items = response['Items']

            # it is possible that the query returned only a partial list of the user's archivals
            # if so, keep querying until all archivals are retrieved
            # @ref https://beabetterdev.com/2022/02/07/how-to-query-dynamodb-with-boto3/ search for "pagination"
            while 'LastEvaluatedKey' in response:
                response = table.query(
                    IndexName=secondary_index,
                    ExclusiveStartKey=response['LastEvaluatedKey'],
                    KeyConditionExpression=Key('user_id').eq(user_id),
                    FilterExpression=Attr(glacier_key).exists(),
                    ProjectionExpression=f"{glacier_key}, job_id" # s3_results_bucket, s3_key_result_file"
                )
                items += response['Items']
            
        except ClientError as e:
            message = f"ClientError while querying DynamoDB for user archivals. Error message: {str(e)}. \
                AWS Error message: {e.response['Error']['Message']}. \
                AWS Request ID: {e.response['ResponseMetadata']['RequestId']}. \
                AWS HTTP Status Code: {e.response['ResponseMetadata']['HTTPStatusCode']}"
            print(message)
            app.logger.error(message)

        except Exception as e:
            message = f"Unexpected error while querying DynamoDB for user archivals. Error message: {str(e)}. \
                Error type: {type(e).__name__}"
            print(message)
            app.logger.error(message)

        # try to publish message to SNS topic for each archival
        print("Publishing SNS messages to request restoration of user data from Glacier")
        sns_client = boto3.client('sns', config=Config(signature_version='s3v4'), region_name=app.config["AWS_REGION_NAME"])

        publish_restoration_sns(sns_client, items, user_id, app.config["AWS_SNS_THAW_REQUESTS"])

        # Display confirmation page
        return render_template("subscribe_confirm.html", stripe_id = customer.id)



#-------------------------------------------------------------------------------
# Auxiliary functions
#-------------------------------------------------------------------------------
def delete_s3_file(bucket_name:str, object_key:str):
    """ Send a presigned DELETE request to AWS S3 to delete a file from an S3 bucket
    Args:
        bucket_name (str): The name of the S3 bucket where the file is stored. Eg: 'gas-inputs'
        object_key (str): The key of the file in the S3 bucket. Eg: 'mashalimay/userX/<job_id>~test.vcf'
    """

    # instantiate boto3 client
    s3 = boto3.client('s3', config=Config(signature_version='s3v4'), region_name=app.config['AWS_REGION_NAME'])

    # try to make a presigned DELETE request
        # AWS reference : 
        # 1) https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-presigned-urls.html
        # (see example 'Using presigned URLs to perform other S3 operation')
        # https://docs.aws.amazon.com/AmazonS3/latest/userguide/example_s3_Scenario_PresignedUrl_section.html
        # (search for "client_action" in the python page to see how to fill the ClientMethod)
    try:
        delete_url = s3.generate_presigned_url(
            ClientMethod='delete_object',
            Params={
                'Bucket': bucket_name,
                'Key': object_key
            },
            ExpiresIn=app.config["AWS_SIGNED_REQUEST_EXPIRATION"],  
            HttpMethod='DELETE',  
            )

        response = requests.delete(delete_url)

    # if presigned delete failed and file not deleted from S3, log warning message 
    except ClientError as e:
        message = "WARNING: annotation job failed but S3 input file not deleted. " + \
                f"AWS ClientError message: {e.response['Error']['Message']}" + \
                f"AWS HTTP Status Code: {e.response['ResponseMetadata']['HTTPStatusCode']}" + \
                f"S3 key of file not deleted: {object_key}"
        print(message)
        app.Logger.warning(message)
        
    except Exception as e:
        message = "WARNING: annotation job failed but S3 input file not deleted. " + \
                f"Unexpected error occured. Error message: {str(e)}. Error type: {type(e).__name__}" + \
                f"S3 key of file not deleted: {object_key}"
        print(message)
        app.logger.warning(message)

def delete_dynamo_key(db_client, table_name:str, key_name:str, value:str):
    """ Send a DELETE request to AWS DynamoDB to delete a key-value pair from a table
    Args:
        db_client (s3.client()): The boto3 client for DynamoDB
        table_name (str): the name of the table in DynamoDB
        key_name (str): the name of the key to delete
        value (str): the value of the key to delete
    """
    try:
        # AWS docs with syntax (example in the end of page): https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/delete_item.html
        response = db_client.delete_item(
            TableName=table_name,
            Key = {key_name: {'S': value}}
        )
    except ClientError as e:
        message = "WARNING: annotation job failed but DynamoDB job item not deleted. " + \
                f"AWS ClientError message: {e.response['Error']['Message']}" + \
                f"AWS HTTP Status Code: {e.response['ResponseMetadata']['HTTPStatusCode']}"
        print(message)
        app.Logger.warning(message)
        
    except Exception as e:
        message = "WARNING: annotation job failed but DynamoDB job item not deleted. " + \
            f"Unexpected error occured. Error message: {str(e)}. Error type: {type(e).__name__}"
        print(message)
        app.logger.warning(message)
            
def local_time(epoch_time:int, format = None):
    
    if format=="@":
        return time.strftime('%Y-%m-%d @ %H:%M:%S', time.localtime(epoch_time))

    return time.strftime('%Y-%m-%d %H:%M', time.localtime(epoch_time))

def gen_presigned_url_s3_download(bucket_name:str, object_key:str):
    s3 = boto3.client(
        "s3",
        region_name=app.config["AWS_REGION_NAME"],
        config=Config(signature_version="s3v4"),
    )
    try:
        response = s3.generate_presigned_url(
                    'get_object',
                    Params={'Bucket': bucket_name,
                            'Key': object_key},
                    ExpiresIn=app.config["AWS_SIGNED_REQUEST_EXPIRATION"])
    except ClientError as e:
        raise e

    except Exception as e:
        raise e
    return response


def publish_restoration_sns(sns_client, items:list, user_id:str, topic_arn:str):
    # Publish the message to the SNS topic
    failed_job_ids = []
    for item in items:
        try:
            response = sns_client.publish(
                TopicArn = topic_arn,
                Message = json.dumps(item)
            )
            
        except ClientError as e:
            message = f"WARNING: Failed to publish restoration request. Job ID {item['job_id']}. \
            User {user_id}. SNS topic {topic_arn} AWS ClientError message: {e.response['Error']['Message']}"
            print(message)
            app.logger.warning(message)
            failed_job_ids(item['job_id'])
            continue

        except Exception as e:
            message = f"WARNING: Failed to publish restoration request.Job ID {item['job_id']}. \
            User {user_id}. SNS topic {topic_arn}. Unexpected error. Error message: {str(e)}. Error type: {type(e).__name__}"
            print(message)
            app.logger.warning(message)
            failed_job_ids(item['job_id'])
            continue
    # might be useful returning failed publications for future error handling
    return failed_job_ids 

def cancel_strip_subscription(subscription):
    try:
        #@ref https://stripe.com/docs/api/subscriptions/cancel
        stripe.Subscription.delete(subscription.id)
        print(f"Subscription {subscription.id} canceled successfully")

    except stripe.error.StripeError as e:
        message = f"WARNING: Failed to cancel subscription. Subscription ID {subscription.id}. \
        User {subscription.customer}. Stripe error message: {e.error.message}"
        print(message)
        app.logger.warning(message)

    except Exception as e:
        message = f"WARNING: Failed to cancel subscription. Subscription ID {subscription.id}. \
        User {subscription.customer}. Unexpected error occured. Error message: {str(e)}. Error type: {type(e).__name__}"
        print(message)
        app.logger.warning(message)

"""DO NOT CHANGE CODE BELOW THIS LINE
*******************************************************************************
"""

"""Set premium_user role
"""


@app.route("/make-me-premium", methods=["GET"])
@authenticated
def make_me_premium():
    # Hacky way to set the user's role to a premium user; simplifies testing
    update_profile(identity_id=session["primary_identity"], role="premium_user")
    return redirect(url_for("profile"))


"""Reset subscription
"""


@app.route("/unsubscribe", methods=["GET"])
@authenticated
def unsubscribe():
    # Hacky way to reset the user's role to a free user; simplifies testing
    update_profile(identity_id=session["primary_identity"], role="free_user")
    return redirect(url_for("profile"))


"""Home page
"""


@app.route("/", methods=["GET"])
def home():
    return render_template("home.html"), 200


"""Login page; send user to Globus Auth
"""


@app.route("/login", methods=["GET"])
def login():
    app.logger.info(f"Login attempted from IP {request.remote_addr}")
    # If user requested a specific page, save it session for redirect after auth
    if request.args.get("next"):
        session["next"] = request.args.get("next")
    return redirect(url_for("authcallback"))


"""404 error handler
"""
@app.errorhandler(404)
def page_not_found(e):
    return (
        render_template(
            "error.html",
            title="Page not found",
            alert_level="warning",
            message="The page you tried to reach does not exist. \
      Please check the URL and try again.",
        ),
        404,
    )


"""403 error handler
"""


@app.errorhandler(403)
def forbidden(e):
    return (
        render_template(
            "error.html",
            title="Not authorized",
            alert_level="danger",
            message="You are not authorized to access this page. \
      If you think you deserve to be granted access, please contact the \
      supreme leader of the mutating genome revolutionary party.",
        ),
        403,
    )


"""405 error handler
"""


@app.errorhandler(405)
def not_allowed(e):
    return (
        render_template(
            "error.html",
            title="Not allowed",
            alert_level="warning",
            message="You attempted an operation that's not allowed; \
      get your act together, hacker!",
        ),
        405,
    )


"""500 error handler
"""


@app.errorhandler(500)
def internal_error(error):
    return (
        render_template(
            "error.html",
            title="Server error",
            alert_level="danger",
            message="The server encountered an error and could \
      not process your request.",
        ),
        500,
    )


"""CSRF error handler
"""


from flask_wtf.csrf import CSRFError


@app.errorhandler(CSRFError)
def csrf_error(error):
    return (
        render_template(
            "error.html",
            title="CSRF error",
            alert_level="danger",
            message=f"Cross-Site Request Forgery error detected: {error.description}",
        ),
        400,
    )


### EOF
