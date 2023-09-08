# Import modules
import os
import sys
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from subprocess import Popen, SubprocessError
import json
from configparser import ConfigParser, ExtendedInterpolation

#==============================================================================
# Get configuration
#==============================================================================
config = ConfigParser(os.environ, interpolation=ExtendedInterpolation())
config.read("annotator_config.ini")

# directory to store annotation files
jobs_dir = config.get('ann', 'JobsDir')

# path to the run.py script
anntools_exec_path = config.get('ann', 'AnnToolsExecPath')

# AWS region
aws_region = config.get('aws', 'AwsRegionName')

# DynamoDB table name
dynamo_tbl_name = config.get('gas', 'AnnotationsTable')

# SQS queue URL
queue_url = config.get('sqs', 'QueueURL')
max_messages = config.getint('sqs', 'MaxMessages')
wait_time = config.getint('sqs', 'WaitTime')

#==============================================================================
# Retrieve annotation request from SQS queue and send to AnnTools
#==============================================================================
def request_annotation():
    sqs_client = boto3.client('sqs', config=Config(signature_version='s3v4'), region_name=aws_region)
    while True:
        # ----------------------------------------------------------------- 
        # try to retrieve messages from the SQS queue
        # -----------------------------------------------------------------
        try:
            # @ref AWS docs with syntax for receive_message method: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/receive_message.html#
            # AWS docs explaining short vs long polling: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-long-polling.html
            response = sqs_client.receive_message(
                QueueUrl = queue_url,
                MaxNumberOfMessages = max_messages,
                WaitTimeSeconds= wait_time # long polling = True
            )

        except ClientError as e:
            print(f"ERROR: Error while accessing AWS SQS service.")
            print(f"AWS Error message: {e.response['Error']['Message']}.\n" +
                            f"AWS Request ID: {e.response['ResponseMetadata']['RequestId']}.\n" +
                            f"AWS HTTP Status Code: {e.response['ResponseMetadata']['HTTPStatusCode']}")
            print("The annotation service will be stopped. Please investigate the error cause and restart the service.")
            sys.exit(0)

        for message in response.get('Messages', []):
            # -----------------------------------------------------------------   
            # try to parse message retrieved from SQS queue
            # -----------------------------------------------------------------
            try:
                job_data = json.loads(json.loads(message['Body'])['Message'])
                job_id = job_data['job_id']
                s3_inputs_bucket = job_data['s3_inputs_bucket']
                s3_key = job_data['s3_key_input_file']                    # eg: mashalimay/userX/<job_id>~test.vcf 
                s3_filename = os.path.basename(s3_key)                    # eg: <job_id>~test.vcf
                prefix = job_data['prefix'] + job_data['user_id'] + '/'   # eg: mashalimay/userX/
                user_id = job_data['user_id']
                #email = job_data['email']      # use this if want to pass email through SQS instead of PostGres database
                #role = job_data['role']            

            except KeyError as e:
                # if message is missing critical fields, delete it from the queue and continue to next message
                print(f"WARNING: Key Error while parsing SNS message {message['MessageId']}. Message is missing critical fields: {str(e)}.")
                delete_SQS_message(sqs_client, message,
                        f"WARNING: Message message{'MessageId'} with invalid data, but unable to remove it from queue due to AWS SQS service error.")
                
                continue
            
            except Exception as e:
                # unexpected errors while parsing message => do not know cause
                # so do not delete from queue and continue to next message
                print_warning_message(message['MessageId'], job_id, 
                                    f"Unexpected error. Error message: {str(e)}. Error type: {type(e).__name__}")
                continue
                
            # -----------------------------------------------------------------
            # create a unique folder for the annotation job in the server machine
            # -----------------------------------------------------------------
            
            job_directory = os.path.join(jobs_dir, job_id) # eg: ~/jobs/<job_id>/
            try:       
                os.makedirs(job_directory)
            except FileExistsError as e:
                # if job directory already exists, it might be because of a subprocessError 
                # try to run annotation job again to guarantee that it will be processed
                # TODO check if the log files are in the S3 bucket before deciding to run the job again
                
                print(f"WARNING: FileExistsError while creating job directory. Error message: {str(e)}. Error type: {type(e).__name__}")
                print("Will run annotation job again for assurance.")

            except OSError as e:
                print_warning_message(message['MessageId'], job_id, 
                        f"OSError while creating job directory. Error message: {str(e)}. Error type: {type(e).__name__}")
                continue

            except Exception as e:
                print_warning_message(message['MessageId'], job_id,
                    f"Unexpected error while creating job directory. Error message: {str(e)}. Error type: {type(e).__name__}")
                continue
            
            # full path to the input file in local machine (eg: home/ubuntu/jobs/<job_id>/<job_id>~test.vcf)
            full_path_input_file = os.path.join(job_directory, s3_filename)

            # -----------------------------------------------------------------  
            # download the input file from the S3 bucket to local machine
            # -----------------------------------------------------------------
            
            s3 = boto3.resource('s3', config=Config(signature_version='s3v4'), region_name=aws_region)
            try:
                s3.Bucket(s3_inputs_bucket).download_file(s3_key, full_path_input_file)

            # if download fails, print warning message and continue to next message
            # does not delete message from queue because it might be a temporary error
            except ClientError as e:
                if e.response['Error']['Code'] == "NoSuchKey":
                    print_warning_message(message['MessageId'], job_id,
                            f"File {s3_filename} not found in bucket {s3_inputs_bucket}.")
                    
                    continue
                elif e.response['Error']['Code'] == "NoSuchBucket":
                    print_warning_message(message['MessageId'], job_id,
                            f"Bucket {s3_inputs_bucket} not found.")
                    
                    continue
                
                elif e.response['Error']['Code'] == "404":
                    # Obs: sometimes when a file is not found, be it because of the bucket or key, 
                    # the response code ends up being 404, instead of the more specific exceptions above. 
                    # Therefore, catching code '404' separately in case it is one of the two cases.
                    print_warning_message(message['MessageId'], job_id,
                            f"AWS Error: unable to find file {s3_filename} in bucket {s3_inputs_bucket}.\n"+
                            f"Check if key and bucket name are correct and investigate error details.\n" +
                            f"AWS Error message: {e.response['Error']['Message']}.\n" +
                            f"AWS Request ID: {e.response['ResponseMetadata']['RequestId']}.")
                    continue
                    
                else:
                    # if no specific error code caught, provide information that can be helpful
                    # following guidelines in https://boto3.amazonaws.com/v1/documentation/api/latest/guide/error-handling.html
                    print_warning_message(message['MessageId'], job_id,
                            f"Unrecognized error in AWS client when downloading file {s3_filename} from bucket {s3_inputs_bucket}.\n"
                            f"Check if key and bucket name are correct and investigate error details.\n" +
                            f"AWS Error message: {e.response['Error']['Message']}.\n" +
                            f"AWS Request ID: {e.response['ResponseMetadata']['RequestId']}.\n" +
                            f"AWS HTTP Status Code: {e.response['ResponseMetadata']['HTTPStatusCode']}"
                    )
                    continue

            #-------------------------------------------------------------------
            # Run the annotation job as a subprocess
            #-------------------------------------------------------------------
            try:
                Popen([sys.executable, anntools_exec_path, full_path_input_file, job_id, prefix, user_id])

                # remove message from queue
                delete_SQS_message(sqs_client, message,
                    f"WARNING: Job '{job_id}' successfully submitted for annotation, +"
                    f"but unable to remove message '{message['MessageId']}' from SQS queue")

            # if subprocess fails, send error message to user and exit
            # does not delete message from queue because it might be a temporary error
            except SubprocessError as e:
                print_warning_message(message['MessageId'], job_id,
                        f"SubprocessError while creating subprocess for annotation job. " +
                        f"Error message: {str(e)}. Error type: {type(e).__name__}")
                continue
                
            except Exception as e:
                print_warning_message(message['MessageId'], job_id,
                        f"Unexpected error while creating subprocess for annotation job. " +
                        f"Error message: {str(e)}. Error type: {type(e).__name__}")
                continue
                
            #------------------------------------------------------------------------------------------
            # Update Database: update the status of the job to 'RUNNING' if current status is 'PENDING'
            #------------------------------------------------------------------------------------------
            # @ref to update item using boto3.client: 
            #   AWS docs (follow syntax and example in the end): https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/update_item.html
            #   Some concrete examples to get used to syntax: https://binaryguy.tech/aws/dynamodb/update-items-in-dynamodb-using-python/
            # @ref for conditional update: (there is not direct references for this, have to mix a couple of sources)
            #   AWS docs gives a good description of the UpdateExpression ConditionExpression ExpressionAttributeNames and ExpressionAttributeValues parameters
            #   It also explains why using "#" might be desirable (searc for "#" in the page):
            #     https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/update_item.html
            #   these two link have some simple examples using these fields (boto3.resource, but same syntax basically)
            #     https://linuxhint.com/conditional-updates-dynamodb/
            #     https://www.youtube.com/watch?v=EacuuiQvJvg (jumo to ~24:39)
            
            
            dynamodb = boto3.client('dynamodb', config=Config(signature_version='s3v4'), region_name= aws_region)
            try:
                db_response = dynamodb.update_item(
                        TableName=dynamo_tbl_name,
                        Key={'job_id': {'S': job_id}},
                        UpdateExpression='SET #annot_status = :new_status',
                        ConditionExpression='#annot_status = :old_status',
                        ExpressionAttributeNames={'#annot_status': 'job_status'},
                        ExpressionAttributeValues={
                            ':new_status': {'S': 'RUNNING'},
                            ':old_status': {'S': 'PENDING'}
                        }
                )
                print(f"Job status for job {job_id} was updated to 'RUNNING' in the database.")

            # if update of database fails, send warning message
            except ClientError as e:
                if e.response['Error']['Code'] == "ConditionalCheckFailedException":
                    print(f"WARNING: error while updating database for job {job_id} from message '{message['MessageId']}'.\n" + \
                        f"Job annotation was created, but the job status could not be updated in the database.\n"+
                        f"Job status was not 'PENDING' when job was submited.")
                    
                else:
                    print(f"WARNING: error while updating database for job '{job_id}' from message '{message['MessageId']}'.\n" + \
                        f"Job annotation was created, but the job status could not be updated in the database.\n"+
                        f"AWS Client error: {e.response['Error']['Message']}. AWS HTTP status code: {e.response['ResponseMetadata']['HTTPStatusCode']}.")


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
        print(f"Message {message['MessageId']} was removed from queue.")

    except ClientError as e:
        if warning_message:
            print(warning_message + " due to AWS SQS service error.")
            print(f"AWS Error message: {e.response['Error']['Message']}.\n" +
                f"AWS Request ID: {e.response['ResponseMetadata']['RequestId']}.\n" +
                f"AWS HTTP Status Code: {e.response['ResponseMetadata']['HTTPStatusCode']}")            

    except Exception as e:
        if warning_message:
            print(warning_message + " due to unexpected error.")
            print(f"Error message: {str(e)}. Error type: {type(e).__name__}")


def print_warning_message(message_id, job_id, additional_message=""):
    """ Prints a warning message with useful information for error handling.
    Args:
        message_id (str): the message id in the SQS queue
        job_id (str): the job id associated to the message
        additional_message (str, optional): an additional message to include in the warning. Typically with information for error handling. Defaults to "".
    """
    
    print(f"WARNING: error while processing SNS message '{message_id}' associated to job '{job_id}'.")
    if additional_message:
        print(additional_message)
    print("Message will not be removed from queue and will try to be parsed again. Please investigate error cause.")


if __name__ == '__main__':
    request_annotation()
    



