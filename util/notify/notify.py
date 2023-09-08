# notify.py
#
# Notify users of job completion
#
# Copyright (C) 2015-2023 Vas Vasiliadis
# University of Chicago
##
__author__ = "Vas Vasiliadis <vas@uchicago.edu>"

import boto3
import time
import os
import sys
import json
from botocore.exceptions import ClientError
from botocore.config import Config
from configparser import ConfigParser, ExtendedInterpolation

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

#==============================================================================
# Get configuration
#==============================================================================
config = ConfigParser(os.environ, interpolation=ExtendedInterpolation())
config.read("notify_config.ini")

queue_url = config.get('sqs', 'QueueURL')
gas_subdomain_URL = config.get('gas', 'SubdomainURL')
mail_sender = config.get('gas', 'MailDefaultSender')
aws_region = config.get('aws', 'AwsRegionName')


"""
Reads result messages from SQS and sends notification emails.
"""
def handle_results_queue(sqs=None):
    # ----------------------------------------------------------------- 
    # try to retrieve messages from the SQS queue
    # ----------------------------------------------------------------- 
    try:
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=config.getint('sqs', 'MaxMessages'),
            WaitTimeSeconds=config.getint('sqs', 'WaitTime') # long polling = True
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
            job_results_data = json.loads(json.loads(message['Body'])['Message'])
            job_id = job_results_data['job_id']
            complete_time = job_results_data['complete_time']
            user_id = job_results_data['user_id']
            #email = job_results_data['email'] # use this if want to retrieve email from SQS instead of database
            
        except KeyError as e:
            # if message is missing critical fields, delete it from the queue and continue to next message
            print(f"WARNING: Key Error while parsing SNS message {message['MessageId']}. Message is missing critical fields: {str(e)}.")
            delete_SQS_message(sqs, message,
                    f"WARNING: Message message{'MessageId'} with invalid data, but unable to remove it from queue due to AWS SQS service error.")
            
            continue
        
        except Exception as e:
            # unexpected errors while parsing message => do not know what it is so do not delete from queue and continue to next message
            
            print_warning_message(message['MessageId'], job_id, 
                                f"Unexpected error. Error message: {str(e)}. Error type: {type(e).__name__}")
            continue

        # Try to get user profile and emal from database
        try:
            profile = helpers.get_user_profile(user_id)
            email = profile['email']

        except ClientError as e:
            warning_message = (f"ClientError while extracting user {user_id} data from database. " + 
                        f"AWS Error message: {e.response['Error']['Message']}. " +
                        f"AWS Request ID: {e.response['ResponseMetadata']['RequestId']}. " +
                        f"AWS HTTP Status Code: {e.response['ResponseMetadata']['HTTPStatusCode']}")

            print_warning_message(message['MessageId'], job_id, warning_message)
            continue
        except Exception as e:
            warning_message = (f"Unexpected error while extracting user {user_id} data from database." + 
                        f"Error message: {str(e)}. Error type: {type(e).__name__}")
            
            print_warning_message(message['MessageId'], job_id, warning_message)
            continue

        # -----------------------------------------------------------------
        # try send email to user and delete message from SQS queue
        # -----------------------------------------------------------------
        try:
            url = gas_subdomain_URL+ "/" + job_id
            subject = f"Results available for job {job_id}"
            time = local_time(int(complete_time),"@")
            body = (f"Your annotation job completed at {time}. " +
                f"Click here to view job details and results: {url}")
                    
            response = helpers.send_email_ses(recipients=email, sender=mail_sender, subject=subject, body=body)

            # try to delete message from SQS queue; send warning message if it fails
            print(f"Mail sent to {email} for job {job_id}.")
            delete_SQS_message(sqs, message, 
                    f"WARNING: Email to {email} for job id {job_id} successfuly sent, " +
                    f"but unable to remove message {message['MessageId']} from SQS queue")

        except ClientError as e:
            warning_message = (f"ClientError while sending email to {email} for job {job_id}. " + 
                        f"AWS Error message: {e.response['Error']['Message']}. " +
                        f"AWS Request ID: {e.response['ResponseMetadata']['RequestId']}. " +
                        f"AWS HTTP Status Code: {e.response['ResponseMetadata']['HTTPStatusCode']}")

            print_warning_message(message['MessageId'], job_id, warning_message)
            continue

        except Exception as e:
            warning_message = (f"Unexpected error while sending email to {email} for job {job_id}. " + 
                        f"Error message: {str(e)}. Error type: {type(e).__name__}")
            
            print_warning_message(message['MessageId'], job_id, warning_message)
            continue
                
                
        
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
    
    print(f"WARNING:error while processing SNS message {message_id} associated to job {job_id}.")
    if additional_message:
        print(additional_message)
    print("Message will not be removed from queue and will try to be parsed again. Please investigate error cause.")

# helper function to format time
def local_time(epoch_time:int, format = None):
    if format=="@":
        return time.strftime('%H:%M:%S @ %Y-%m-%d', time.localtime(epoch_time))
    
    return time.strftime('%H:%M %Y-%m-%d', time.localtime(epoch_time))

def main():

    # Get handles to resources
    sqs_client = boto3.client('sqs', config=Config(signature_version='s3v4'), region_name=aws_region)

    # Poll queue for new results and process them
    while True:
        handle_results_queue(sqs=sqs_client)


if __name__ == "__main__":
    main()

### EOF
