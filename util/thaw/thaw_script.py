# thaw_script.py
#
# Thaws upgraded (premium) user data
#
# Copyright (C) 2015-2023 Vas Vasiliadis
# University of Chicago
##
__author__ = "Vas Vasiliadis <vas@uchicago.edu>"

import boto3
import os
import sys
import json
from botocore.exceptions import ClientError
from botocore.config import Config


# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

#==============================================================================
# Get configuration
#==============================================================================
from configparser import ConfigParser, ExtendedInterpolation

config = ConfigParser(os.environ, interpolation=ExtendedInterpolation())
config.read("../util_config.ini")
config.read("thaw_script_config.ini")

queue_url = config.get('sqs', 'QueueURL')
aws_region = config.get('aws', 'AwsRegionName')
glacier_vault_name = config.get('glacier', 'VaultName')

# SNS topic for glacier to notify when restoration is complete
sns_restoration_complete = config.get('sns', 'RestorationCompleteArn')

"""
Initiate thawing of archived objects from Glacier
"""
def handle_thaw_queue(sqs_client=None, glacier_client=None):
        # ----------------------------------------------------------------- 
        # try to retrieve messages from the SQS queue
        # ----------------------------------------------------------------- 
        try:
            #print(f"Retrieving messages from SQS queue {queue_url}...")
            response = sqs_client.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=config.getint('sqs', 'MaxMessages'),
                WaitTimeSeconds=config.getint('sqs', 'WaitTime') # long polling = True
            )

        except ClientError as e:
            print(f"ERROR: Error while accessing AWS SQS service.")
            print(f"AWS Error message: {e.response['Error']['Message']}.\n" +
                            f"AWS Request ID: {e.response['ResponseMetadata']['RequestId']}.\n" +
                            f"AWS HTTP Status Code: {e.response['ResponseMetadata']['HTTPStatusCode']}")
            print("Iteration of thawing service will be stopped. Please investigate the error cause and restart the service.")
            return

        for message in response.get('Messages', []):
            # -----------------------------------------------------------------   
            # try to parse message retrieved from SQS queue
            # -----------------------------------------------------------------
            try:
                print(f"\nRetrieving thaw data from SQS message {message['MessageId']}...")
                thaw_data = json.loads(json.loads(message['Body'])['Message'])
                job_id = thaw_data['job_id']
                #s3_results_bucket = thaw_data['s3_results_bucket']
                #s3_key_result_file = thaw_data['s3_key_result_file']
                archive_id = thaw_data['results_file_archive_id']
                #user_id = thaw_data['user_id']
            except KeyError as e:
                # if message is missing critical fields, delete it from the queue and continue to next message
                print(f"WARNING: Key Error while parsing SNS message {message['MessageId']}. Message is missing critical fields: {str(e)}.")
                print("Message will be deleted from queue.")
                delete_SQS_message(sqs_client, message,
                        f"WARNING: Message {message['MessageId']} with invalid data, but unable to remove it from queue due to AWS SQS service error.")
                continue
            
            except Exception as e:
                # unexpected errors while parsing message => do not know what it is so do not delete from queue and continue to next message
                print_warning_message(message['MessageId'], job_id, 
                                    f"Unexpected error. Error message: {str(e)}. Error type: {type(e).__name__}")
                continue

            # -----------------------------------------------------------------   
            # try to do an expedited retrieval of results file
            # -----------------------------------------------------------------
            job_data = json.dumps({
                'job_id': job_id
                #'s3_results_bucket':s3_results_bucket,
                #'s3_key_result_file':s3_key_result_file
                })
            
            try:
                print("\nAttempting Expedited retrieval...")
                #@ref https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier/client/initiate_job.html
                response = glacier_client.initiate_job(
                                vaultName = glacier_vault_name,
                                jobParameters = {
                                    'Type': 'archive-retrieval',
                                    'ArchiveId': archive_id,
                                    'Description': job_data,
                                    'Tier': 'Expedited',
                                    'SNSTopic': sns_restoration_complete
                                })
                print(f'Expedited retrieval for job {job_id} archive {archive_id} initiated.')

            except ClientError as e:
                if e.response['Error']['Code'] == 'InsufficientCapacityException':
                    print('Expedited retrieval capacity exceeded.\nAttempting Standard retrieval...')
                    try:
                        response = glacier_client.initiate_job(
                                        vaultName = glacier_vault_name,
                                        jobParameters = {
                                            'Type': 'archive-retrieval',
                                            'ArchiveId': archive_id,
                                            'Tier': 'Standard',
                                            'Description': job_data,
                                            'SNSTopic': sns_restoration_complete
                                        })
                        print(f'Standard retrieval for job {job_id} archive {archive_id} initiated.\n')
                    except ClientError as e:
                        print(f"ERROR: Error retrieving results archive {archive_id} for job {job_id} from Glacier.\
                            Expedited and Standard retrieval failed.\n"
                            f"AWS Error message: {e.response['Error']['Message']}.\n" +
                            f"AWS Request ID: {e.response['ResponseMetadata']['RequestId']}.\n" +
                            f"AWS HTTP Status Code: {e.response['ResponseMetadata']['HTTPStatusCode']}")
                        continue
                    except Exception as e:
                        print(f"ERROR: Unexpected error in Standard retrieval for archive {archive_id} for job {job_id} from Glacier.\n")
                        print(f"Error message: {str(e)}. Error type: {type(e).__name__}")
                        continue
                else:
                    print(f"ERROR: AWS error while retrieving results archive {archive_id} for job {job_id} from Glacier.\n"
                        f"AWS Error message: {e.response['Error']['Message']}.\n" +
                        f"AWS Request ID: {e.response['ResponseMetadata']['RequestId']}.\n" +
                        f"AWS HTTP Status Code: {e.response['ResponseMetadata']['HTTPStatusCode']}")
                    continue
                
            except Exception as e:
                print(f"ERROR: Unexpected error during expedited retrieval for archive {archive_id} for job {job_id} from Glacier.\n")
                print(f"Error message: {str(e)}. Error type: {type(e).__name__}")
                continue
                
            # -----------------------------------------------------------------   
            # delete message from SQS queue
            # -----------------------------------------------------------------
            print("Deleting message from SQS queue...")
            delete_SQS_message(sqs_client, message,
                f"WARNING: Thawing request for results of {job_id} successful, but unable to remove message from SQS.")

                        
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


def main():
    # Get handles to resources
    sqs_client = boto3.client('sqs', config=Config(signature_version='s3v4') ,region_name=aws_region)
    glacier_client = boto3.client('glacier', config=Config(signature_version='s3v4') , region_name=aws_region)   

    # Poll queue for new results and process them
    while True:
        handle_thaw_queue(sqs_client, glacier_client)

if __name__ == "__main__":
    main()

### EOF
