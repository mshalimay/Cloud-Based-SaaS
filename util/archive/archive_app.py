# archive_script.py
#
# Archive free user data
#
# Copyright (C) 2015-2023 Vas Vasiliadis
# University of Chicago
##
__author__ = "Vas Vasiliadis <vas@uchicago.edu>"
import requests
import boto3
from botocore.exceptions import ClientError
from botocore.config import Config
import os
import sys
import json
from flask import Flask, jsonify, request
from subprocess import Popen, SubprocessError
import shutil


app = Flask(__name__)
app.url_map.strict_slashes = False

#==============================================================================
# Get configuration
#==============================================================================
# Get configuration and add to Flask app object
environment = "archive_app_config.Config"
app.config.from_object(environment)

# directory to store annotation files
archivals_dir = app.config['ARCHIVALS_DIR'] # e.g. /home/ubuntu/gas/archive/archivals

# # path to the run.py script
# archival_exec_path = app.config['ANNTOOLS_EXEC_PATH']

# AWS region
aws_region = app.config["AWS_REGION_NAME"]

# DynamoDB table name
dynamo_tbl_name = app.config["AWS_DYNAMODB_ANNOTATIONS_TABLE"]

# SQS queue URL
queue_url = app.config["SQS_ARCHIVAL_REQUESTS_QUEUE_URL"]
sqs_max_messages = app.config["AWS_SQS_MAX_MESSAGES"]
sqs_wait_time = app.config["AWS_SQS_WAIT_TIME"]

# glacier vault
glacier_vault_name = app.config["VAULT_NAME"]

# Instantiate AWS service clients
sqs_client = boto3.client('sqs', config=Config(signature_version='s3v4'), region_name=aws_region)
s3_client = boto3.client('s3', config=Config(signature_version='s3v4'), region_name=aws_region)
glacier = boto3.client('glacier', config=Config(signature_version='s3v4'), region_name=aws_region) 

# presigned post expiration
presigned_post_expiration = app.config["AWS_S3_SIGNED_REQUEST_EXPIRATION"]


# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers


#==============================================================================
# Endpoints
#==============================================================================
@app.route("/", methods=["GET"])
def home():
    return f"This is the Archive utility: POST requests to /archive."

@app.route("/archive", methods=["GET", "POST"])
def process_job_request():

    print(request)

    if request.method == "GET":

        return jsonify({"code": 405, "error": "Expecting SNS POST request."}), 405

    elif request.method == "POST":
    # @ref To parse SNS POST request, refer to Step 1 page in this AWS guide:
    # https://docs.aws.amazon.com/sns/latest/dg/SendMessageToHttp.prepare.html

        # Check SNS POST message type
        try:
            message_type = request.headers["x-amz-sns-message-type"]
        except KeyError as e:
            return (
                jsonify({"code": 500, "error": f"SNS publication is missing key fields. Error message: str{e}"}),
                500)

        except Exception as e:
            return (
                jsonify({"code": 500, "error": f"Unexpected error when retrieving message type. Error message: str{e}"}),
                500,
            )

        # if SNS message received is to subscribe, confirm subscription 
        if message_type == "SubscriptionConfirmation":
            message_data = json.loads(request.data)

            try:
                subs_url = message_data["SubscribeURL"]
                response = requests.get(subs_url)

            except requests.RequestException as e:
                return (
                    jsonify({"code": 500, "error": f"Error sending request to subscription URL. Error message: str{e}"}), 500)

            except KeyError as e:
                return (jsonify({"code": 500, 
                             "error": f"KeyError while confirming subscription for SNS topic. Error message: str{e}"}), 500)

            except Exception as e:
                return (jsonify({"code": 500, 
                             "error": f"Unexpected error while confirming subscription for SNS topic. \
                                 Error message: {str(e)}. Error type: {type(e).__name__}"}), 500)

            if response.status_code == 200:
                return jsonify({"code": 200, "message": "SNS topic subscribed."}), 200
            else:
                return (jsonify({"code": 500, 
                             "error": f"AWS server did not respond to subscription request. \
                                 request status returned from request: {response.status_code}"}), 500)

        else: #if notification type
            # try to deploy annotation job; if any internal error, return 500
            # obs.: from the endpoint perspective, errors in the annotator need not to be specific;
            # more information about errors will be printed to the server cmd
            try:
                message = handle_archive_queue()
            except Exception as e:
                print(str(e))
                print("Error while processing archival request.")
                return (
                    jsonify({"code": 500, "error": f"Internal error while processing archival request. Error message: str{e}"}),
                    500)
            else:
                return (jsonify({"code": 201, "message": "Archival request processed."}), 201)


def handle_archive_queue():
    # ----------------------------------------------------------------- 
    # try to retrieve messages from the SQS queue
    # ----------------------------------------------------------------- 
    try:
        # @ref AWS docs with syntax for receive_message method: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/receive_message.html#
        # AWS docs explaining short vs long polling: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-long-polling.html
        response = sqs_client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=sqs_max_messages,
            WaitTimeSeconds=sqs_wait_time # long polling = True
        )
        print(f"INFO: Received {len(response.get('Messages', []))} messages from SQS queue.")

    except ClientError as e:
        print(f"ERROR: Error while accessing AWS SQS service.")
        print(f"AWS Error message: {e.response['Error']['Message']}.\n" +
                        f"AWS Request ID: {e.response['ResponseMetadata']['RequestId']}.\n" +
                        f"AWS HTTP Status Code: {e.response['ResponseMetadata']['HTTPStatusCode']}")
        raise e

    for message in response.get('Messages', []):
        # -----------------------------------------------------------------   
        # try to parse message retrieved from SQS queue
        # -----------------------------------------------------------------
        try:
            archival_data = json.loads(json.loads(message['Body'])['Message'])
            job_id = archival_data['job_id']
            s3_results_bucket = archival_data['s3_results_bucket']
            s3_key = archival_data['s3_key_result_file']              # eg: mashalimay/userX/<job_id>~test.annot.vcf
            s3_filename = os.path.basename(s3_key)                    # eg: <job_id>~test.annot.vcf
            user_id = archival_data['user_id']

        except KeyError as e:
            # if message is missing critical fields, delete it from the queue and continue to next message
            print(f"WARNING: Key Error while parsing SNS message {message['MessageId']}. Message is missing critical fields: {str(e)}.")
            print("Message will be deleted from queue.")
            delete_SQS_message(sqs_client, message,
                    f"WARNING: Message message{'MessageId'} with invalid data, but unable to remove it from queue due to AWS SQS service error.")
            continue
        
        except Exception as e:
            # unexpected errors while parsing message => do not know what it is so do not delete from queue and continue to next messages
            print(f"WARNING: Unexpected error while parsing SNS message {message['MessageId']}. Error message: {str(e)}. Error type: {type(e).__name__}")
            print("Message will be deleted from queue. Please check logs for more information.")
            delete_SQS_message(sqs_client, message,
                    f"WARNING: Message message{'MessageId'} with invalid data, but unable to remove it from queue due to AWS SQS service error.")
            continue

        # -----------------------------------------------------------------
        # Try to get user role from database to decide on archival
        # -----------------------------------------------------------------
        try:
            profile = helpers.get_user_profile(user_id)
            role = profile['role']

        except ClientError as e:
            warning_message = (f"ClientError while extracting user {user_id} role from database. " + 
                        f"AWS Error message: {e.response['Error']['Message']}. " +
                        f"AWS Request ID: {e.response['ResponseMetadata']['RequestId']}. " +
                        f"AWS HTTP Status Code: {e.response['ResponseMetadata']['HTTPStatusCode']}")

            print_warning_message(message['MessageId'], job_id, warning_message)
            print("Archival will try to be processed again.")
            continue
        
        except Exception as e:
            warning_message = (f"Unexpected error while extracting user {user_id} role from database." + 
                        f"Error message: {str(e)}. Error type: {type(e).__name__}")
            
            print_warning_message(message['MessageId'], job_id, warning_message)
            print("Archival will try to be processed again.")
            continue

        # if user have updated to premium in the meantime, do not archive it's data
        if "premium" in role:
            print(f"User {user_id} upgraded to premium. Archival for job {job_id} \
            will not be processed and SQS message will be deleted.")
            delete_SQS_message(sqs_client, message,
                f"WARNING: user {user_id} upgraded to premium, but couldn't remove archival message {message['MessageId']}")
            continue

        # ---------------------------------------------------------------------
        # create a unique folder for archivals
        # ---------------------------------------------------------------------        
        archival_directory = os.path.join(archivals_dir, job_id) # eg: home/ubuntu/gas/archive/archivals/<job_id>/
        try:
            os.makedirs(archival_directory)
        except FileExistsError as e:
            # try to run archival again to guarantee that it will be processed            
            print(f"WARNING: FileExistsError while creating archival directory. Error message: {str(e)}. Error type: {type(e).__name__}")
            print("Archival will proceed but S3 result file might be overwritten.")

        except OSError as e:
            print_warning_message(message['MessageId'], job_id, 
                f"OSError while creating archival directory. Error message: {str(e)}. Error type: {type(e).__name__}")
            continue
        except Exception as e:
            print_warning_message(message['MessageId'], job_id,
                f"Unrecognized internal error while creating archival directory. Error message: {str(e)}. Error type: {type(e).__name__}")
            continue
        
        # full path to result file in local machine (eg: home/ubuntu/archivals/<job_id>/<job_id>~test.annot.vcf)
        full_path_result_file = os.path.join(archival_directory, s3_filename)

        #-----------------------------------------------------------------------
        # download file from S3
        #-----------------------------------------------------------------------
        # try to download file from S3
        try:
            print(f"Downloading file {s3_filename} from bucket {s3_results_bucket} to {full_path_result_file}.")
            s3_client.download_file(s3_results_bucket, s3_key, full_path_result_file)

        # if download fails, print warning message and continue to next message
        # does not delete message from queue because it might be a temporary error
        except ClientError as e:
            if e.response['Error']['Code'] == "NoSuchKey":
                print_warning_message(message['MessageId'], job_id,
                        f"File {s3_filename} not found in bucket {s3_results_bucket}.")
                
                continue
            elif e.response['Error']['Code'] == "NoSuchBucket":
                print_warning_message(message['MessageId'], job_id,
                        f"Bucket {s3_results_bucket} not found.")
                
                continue
            
            elif e.response['Error']['Code'] == "404":
                # Obs: sometimes when a file is not found, be it because of the bucket or key, 
                # the response code ends up being 404, instead of the more specific exceptions above. 
                # Therefore, catching code '404' separately in case it is one of the two cases.
                print_warning_message(message['MessageId'], job_id,
                        f"AWS Error: unable to find file {s3_filename} in bucket {s3_results_bucket}.\n"+
                        f"Check if key and bucket name are correct and investigate error details.\n" +
                        f"AWS Error message: {e.response['Error']['Message']}.\n" +
                        f"AWS Request ID: {e.response['ResponseMetadata']['RequestId']}.")
                continue
                
            else:
                # if no specific error code caught, provide information that can be helpful
                # following guidelines in https://boto3.amazonaws.com/v1/documentation/api/latest/guide/error-handling.html
                print_warning_message(message['MessageId'], job_id,
                        f"Unrecognized error in AWS client when downloading file {s3_filename} from bucket {s3_results_bucket}.\n"
                        f"Check if key and bucket name are correct and investigate error details.\n" +
                        f"AWS Error message: {e.response['Error']['Message']}.\n" +
                        f"AWS Request ID: {e.response['ResponseMetadata']['RequestId']}.\n" +
                        f"AWS HTTP Status Code: {e.response['ResponseMetadata']['HTTPStatusCode']}"
                )
                continue
       
        #-----------------------------------------------------------------------
        # try to upload file to glacier
        #-----------------------------------------------------------------------
        glacier = boto3.client("glacier", config=Config(signature_version='s3v4'), region_name=aws_region)

        try:
            with open(full_path_result_file, 'rb') as f:
                response = glacier.upload_archive(
                    vaultName = glacier_vault_name,
                    body = f
                )
            results_file_archive_id = response['archiveId']
            print(f"Result file for job {job_id} uploaded to glacier with archive ID {results_file_archive_id}.")
            
        except ClientError as e:
            # if no specific error code, provide information that can be helpful
            error_msg = f"AWS error while uploading result file to glacier. \n" \
                        f"AWS Error message: {e.response['Error']['Message']}\n" \
                        f"Request ID: {e.response['ResponseMetadata']['RequestId']}\n" \
                        f"HTTP code: {e.response['ResponseMetadata']['HTTPStatusCode']}\n"
            # save error log to archival directory, print to console and delete files from EC2 (if specified)
            handle_updload_error(error_msg, archival_directory, files_to_remove = None)
            continue

        except OSError as e:
            error_msg = f"Error opening downloaded S3 file while uploading file to glacier. \
                Error message: {str(e)}. Error type: {type(e).__name__}"
            # save error log to archival directory, print to console and delete files from EC2 (if specified)
            handle_updload_error(error_msg, archival_directory, files_to_remove = None)
            continue
        
        except Exception as e:
            error_msg = f"Unexpected while uploading result file to glacier. \
                Error message: {str(e)}. Error type: {type(e).__name__}"
            # save error log to archival directory, print to console and delete files from EC2 (if specified)
            handle_updload_error(error_msg, archival_directory, files_to_remove = None)
            continue

        #-----------------------------------------------------------------------
        # No upload errors => remove job directory from EC2, update database, 
        #                     delete message from queue, delete file from S3.
        #-----------------------------------------------------------------------
        else:
            # delete the job directory and files
            try:
                print("Cleaning up EC2 directories")
                shutil.rmtree(archival_directory)
            except OSError as e:
                print(f"WARNING: archival of results of job {job_id} completed successfuly but could not delete directory from EC2 instance.")
                print(f"OSError: could not delete directory {archival_directory} from EC2 instance. Error message: {str(e)}. Error type: {type(e).__name__}")
            except Exception as e:
                print(f"WARNING: archival of results of job {job_id} completed successfuly but could not delete directory from EC2 instance.")
                print(f"Unexpected Error: could not delete directory {archival_directory} from EC2 instance. Error message: {str(e)}. Error type: {type(e).__name__}")
           
            # update the DynamoDB database
            print("Updating DynamoDB database.")
            update_dynamoDB(dynamo_tbl_name, job_id, results_file_archive_id, aws_region)

            # delete message from SQS archival queue
            print("Deleting message from SQS archival queue")
            delete_SQS_message(sqs_client, message, 
                    warning_message = f"WARNING: archival of results of job {job_id} completed successfuly, " +
                    f"but SQS message could not be deleted. ")
            
            # delete file from S3 results bucket
            print("Deleting result file from S3.")
            delete_s3_file(s3_client, s3_results_bucket, s3_key, presigned_post_expiration,
                warning_message = f"WARNING: archival of results of job {job_id} \
            completed successfuly but unable to delete result file from S3.")
            
            
#==============================================================================
# Helper functions 
#==============================================================================
def delete_SQS_message(sqs_client, message, warning_message=""):
    """ Try to delete a message from the SQS queue. If it fails, print a warning message
    and useful information for error handling.

    Args:
        sqs_client (boto3.client('sqs')): _description_
        message (dict): item in sqs_client.receive_message()['Messages'] 
        warning_message (str, optional): a warning message to be printed in case of error
    """
    try:
        # @ref syntax for sqs_client.delete_message from boto3 docs: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/delete_message.html
        sqs_client.delete_message(
        QueueUrl=queue_url,
        ReceiptHandle=message['ReceiptHandle'])
        print(f"Archival processed. Message {message['MessageId']} was removed from queue.")

    except ClientError as e:
        if warning_message:
            print(warning_message + " AWS SQS service error.")
            print(f"AWS Error message: {e.response['Error']['Message']}.\n" +
                f"AWS Request ID: {e.response['ResponseMetadata']['RequestId']}.\n" +
                f"AWS HTTP Status Code: {e.response['ResponseMetadata']['HTTPStatusCode']}")            

    except Exception as e:
        if warning_message:
            print(warning_message + " Unexpected error.")
            print(f"Error message: {str(e)}. Error type: {type(e).__name__}")


def print_warning_message(message_id, job_id, additional_message=""):
    """ Prints a warning message with useful information for error handling.
    Args:
        message_id (str): the message id in the SQS queue
        job_id (str): the job id associated to the message
        additional_message (str, optional): an additional message to include in the warning. Typically with information for error handling. Defaults to "".
    """
    
    print(f"WARNING:error while processing SNS message {message_id} associated to job {job_id}.")
    if additional_message:
        print(additional_message)
    print("Message will not be removed from queue and will try to be parsed again. Please investigate error cause.")

def update_dynamoDB(dynamo_tbl_name, job_id, results_file_archive_id, aws_region):
    dynamodb = boto3.client('dynamodb', config=Config(signature_version='s3v4'), region_name=aws_region)
    
    try:
        response = dynamodb.update_item(
            TableName= dynamo_tbl_name,
            Key={'job_id': {'S': job_id}},
            UpdateExpression= 'SET #results_file_archive_id = :results_file_archive_id',
            ExpressionAttributeNames={
                '#results_file_archive_id': 'results_file_archive_id'
            },
            ExpressionAttributeValues={
                ':results_file_archive_id': {'S': results_file_archive_id}
            }
        )
        print("DynamoDB database updated.")        
    except ClientError as e:
        print(f"WARNING: archival of results of job {job_id} completed successfuly, " +
        f"but DynamoDB could not be updated with glacier ID {results_file_archive_id}." +
        f"AWS Error message: {e.response['Error']['Message']}. " +
        f"AWS Request ID: {e.response['ResponseMetadata']['RequestId']}. " +
        f"AWS HTTP Status Code: {e.response['ResponseMetadata']['HTTPStatusCode']}")

def remove_files(file_list):
    for f in file_list:
        try:
            os.remove(f)
        except OSError as e:
            print(f"Error: could not remove file. OSError message: {str(e)}. Error type: {type(e).__name__}")
        except Exception as e:
            print(f"Unexpected Error: could not remove file. Error message: {str(e)}. Error type: {type(e).__name__}")

def handle_updload_error(error_msg, result_file_dir, files_to_remove=None):
    """ Save a log of the error, print a message to the console and remove files if specified.
    """
    try:
        with open(f"{result_file_dir}/error_log.txt", "w") as f:
            f.write(error_msg)

        print(error_msg)
        print("An error log has been saved to the archival task directory.")

        if files_to_remove:
            remove_files(files_to_remove)

    except OSError as e:
        print(f"OSError: could not write error log to file. Error message: {str(e)}. Error type: {type(e).__name__}")

    except Exception as e:
        print(f"Unexpected Error: could not write error log to file. Error message: {str(e)}. Error type: {type(e).__name__}")

def delete_s3_file(s3_client, bucket_name:str, object_key:str,
                   signed_req_expiration:int, warning_message:str=""):
    """ Send a presigned DELETE request to AWS S3 to delete a file from an S3 bucket
    Args:
        s3_client (boto3.client('s3')): boto3 s3 client (not boto3 resource!)
        bucket_name (str): The name of the S3 bucket where the file is stored. Eg: 'gas-inputs'
        object_key (str): The key of the file in the S3 bucket. Eg: 'mashalimay/userX/<job_id>~test.vcf'
        signed_req_expiration (int): number of seconds for the presigned delete request to expire.
    """

    # try to make a presigned DELETE request
        # AWS reference : 
        # 1) https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-presigned-urls.html
        # (see example 'Using presigned URLs to perform other S3 operation')
        # https://docs.aws.amazon.com/AmazonS3/latest/userguide/example_s3_Scenario_PresignedUrl_section.html
        # (search for "client_action" in the python page to see how to fill the ClientMethod)
    try:
        delete_url = s3_client.generate_presigned_url(
            ClientMethod='delete_object',
            Params={
                'Bucket': bucket_name,
                'Key': object_key
            },
            ExpiresIn= signed_req_expiration,
            HttpMethod='DELETE',  
        )

        response = requests.delete(delete_url)

        if response.status_code != 204:
            message = warning_message + \
            f"\nRequest to the URL failed. Check the S3 key or bucket input. \
            \nPresigned request response: {response.status_code}"
            print(message)

        else:
            print(f"File {object_key} deleted from S3 bucket {bucket_name}.")

        return

    # if presigned delete failed and file not deleted from S3, log warning message 
    except ClientError as e:
        message = warning_message + \
                f"\nAWS ClientError message: {e.response['Error']['Message']}" + \
                f"AWS HTTP Status Code: {e.response['ResponseMetadata']['HTTPStatusCode']}" + \
                f"S3 key of file not deleted: {object_key}"
        print(message)
        
    except Exception as e:
        message = warning_message + \
                f"\nUnexpected error occured. Error message: {str(e)}. Error type: {type(e).__name__}" + \
                f"S3 key of file not deleted: {object_key}"
        print(message)

    


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)