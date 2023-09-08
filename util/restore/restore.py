# restore.py
#
# Restores thawed data, saving objects to S3 results bucket
# NOTE: This code is for an AWS Lambda function
#
# Copyright (C) 2015-2023 Vas Vasiliadis
# University of Chicago
##

import json
import boto3
from botocore.exceptions import ClientError
from botocore.config import Config
import os
from configparser import ConfigParser, ExtendedInterpolation

# get configuration
config = ConfigParser(os.environ, interpolation=ExtendedInterpolation())
config.read("config.ini")

region = config.get('aws','AwsRegionName')
glacier_vault_name = config.get('glacier', 'VaultName')
dynamo_tbl_name = config.get('dynamodb', 'AnnTableName')

def lambda_handler(event, context):
    if event:
        batch_item_failures = []
        sqs_batch_response = {}
        
        # instantiate AWS clients
        s3_client = boto3.client('s3', config=Config(signature_version='s3v4'), region_name = region)
        glacier_client = boto3.client('glacier', config=Config(signature_version='s3v4'), region_name = region)
        dynamodb = boto3.resource('dynamodb', config=Config(signature_version='s3v4'), region_name = region)
    
        print('Processing SQS messages...')
        for record in event["Records"]:
            print(record)
            
            #-------------------------------------------------------------------
            # Parse data from SQS
            #-------------------------------------------------------------------
            print("Retrieving data from SQS message")
            try:
                data =  json.loads(json.loads(record['body'])['Message'])
                job_id_glacier = data['JobId']
                job_data = json.loads(data['JobDescription'])
                
                # get job_id associated to the result file
                job_id = job_data['job_id']
                    
                ## get s3 key/bucket where it resultes were stored
                ## obs: use if s3 bucket/key is fixed between archival and restoration
                ## otherwise, query from DynamoDB using job_id
                #s3_results_bucket = job_data['s3_results_bucket']
                #s3_key_result_file = job_data['s3_key_result_file']

            except KeyError as e:
                print(f"Error while parsing SQS message. Message missing critical fields. Error message: {str(e)}")
                print("Please investigate the error cause. This SQS message will not be processed again")
                continue

            except Exception as e:
                print(f"Unexpected error parsing SQS message. Error message: {str(e)}. Error type: {type(e).__name__}")
                print("Please investigate the error cause. This SQS message will not be processed again")
                continue
                
            print(f"Data parsed from SQS. Job id of result file {job_id}")
            
            #-------------------------------------------------------------------
            # Get output from glacier
            #-------------------------------------------------------------------
            print("Getting output from Glacier")
            try:
                #@ref https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier/client/get_job_output.html
                response = glacier_client.get_job_output(
                    jobId = job_id_glacier,
                    vaultName = glacier_vault_name)
    
            except ClientError as e:
                print(f"Error while getting output for glacier job id {job_id_glacier}.")
                print(f"ClientError exception. AWS Error message: {e.response['Error']['Message']}")
                print(f"SQS message will try to be processed in the next batch.")
                batch_item_failures.append({"itemIdentifier": record['messageId']})
                continue
    
            except Exception as e:
                print(f"Unexpected error while getting output for glacier job id {job_id_glacier}")
                print(f"Error message: {str(e)}. Error type: {type(e).__name__}")
                print("Please investigate the error cause. This SQS message will not be processed again")
                #batch_item_failures.append({"itemIdentifier": record['messageId']})
                continue
            
            #-------------------------------------------------------------------
            # Read output from Glacier
            #-------------------------------------------------------------------
            print("Reading glacier output")
            # parse the output and put it back in S3
            try:
                file_bytes = response['body'].read()
                
            except Exception as e:
                print(f"Unexpected error while parsing output for glacier job id {job_id_glacier}")
                print(f"Error message: {str(e)}. Error type: {type(e).__name__}")
                print("Please investigate the error cause. This SQS message will not be processed again")
                continue
            
            print("Glacier output read sucessfully")
            
            #-------------------------------------------------------------------
            # Get S3 key and bucket from DynamoDB
            # Obs: if S3 key and results are fixed between archival and restoration
            # can retrive S3 key and results from the description field
            #-------------------------------------------------------------------
            try:
                print(f"Getting S3 bucket and key for job {job_id} from DynamoDB")
                table = dynamodb.Table('mashalimay_annotations')

                # Query the table using the job_id
                response = table.get_item(
                    Key={"job_id": job_id})
                item = response['Item']
                s3_results_bucket = item['s3_results_bucket']
                s3_key = item['s3_key_result_file']
                
            except ClientError as e:
                print(f"Error while getting S3 results bucket and key from DynamoDB for glacier job {job_id_glacier}.")
                print(f"ClientError exception. AWS Error message: {e.response['Error']['Message']}")
                print(f"SQS message will try to be processed in the next batch.")
                batch_item_failures.append({"itemIdentifier": record['messageId']})
                continue
            except Exception as e:
                print(f"Error while getting S3 results bucket and key from DynamoDB for glacier job {job_id_glacier}.")
                print(f"Error message: {str(e)}. Error type: {type(e).__name__}")
                print("Please investigate the error cause. This SQS message will not be processed again")
                #batch_item_failures.append({"itemIdentifier": record['messageId']})
                continue
            
            #-------------------------------------------------------------------
            # Put object into S3
            #-------------------------------------------------------------------
            print(f"Uploading result file for job {job_id} back to S3")
            try:
                s3_response = s3_client.put_object(
                    Body = file_bytes,
                    Bucket = s3_results_bucket,
                    Key = s3_key
                )
                print("Result file sucessfully uploaded to S3")
            except ClientError as e:
                print(f"Error while putting output into S3 for glacier job id {job_id_glacier}.")
                print(f"ClientError exception. AWS Error message: {e.response['Error']['Message']}")
                print(f"SQS message will try to be processed in the next batch.")
                batch_item_failures.append({"itemIdentifier": record['messageId']})
                continue
            except Exception as e:
                print(f"Error while putting output into S3 for glacier job id {job_id_glacier}")
                print(f"Error message: {str(e)}. Error type: {type(e).__name__}")
                print("Please investigate the error cause. This SQS message will not be processed again")
                #batch_item_failures.append({"itemIdentifier": record['messageId']})
                continue
            
            #-------------------------------------------------------------------
            # Remove glacier results_file_archive_id from DynamoDB
            #-------------------------------------------------------------------
            print("Removing results_file_archive_id attribute from DynamoDB")
            try:
                response = table.update_item(
                    Key={'job_id': job_id},
                    UpdateExpression='REMOVE results_file_archive_id'
                )
            
            except ClientError as e:
                print(f"WARNING: error while removing results_file_archive_id from dynamoDB for job {job_id}.")
                print("DANGER: database in incosistent state.")
                print(f"ClientError exception. AWS Error message: {e.response['Error']['Message']}")
                print(f"SQS message will try to be processed in the next batch.")
                
            except Exception as e:
                print(f"WARNING: error while removing results_file_archive_id from dynamoDB for job {job_id}.")
                print("DANGER: database in incosistent state.")
                print(f"Error message: {str(e)}. Error type: {type(e).__name__}")
                print("Please investigate the error cause. This SQS message will not be processed again")
            
            #-------------------------------------------------------------------
            # Delete file from glacier
            #-------------------------------------------------------------------
            print("Deleting file from glacier")
            archive_id = data['ArchiveId']
            try:
                # @ref https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier/client/delete_archive.html
                response = glacier_client.delete_archive(
                    vaultName = glacier_vault_name,
                    archiveId = archive_id)
                print("Result file sucessfully deleted from glacier")
                print(response)
            
            except ClientError as e:
                print(f"WARNING: Error while deleting result file from glacier for glacier job id {job_id_glacier}.")
                print(f"ClientError exception. AWS Error message: {e.response['Error']['Message']}")
                print("Please investigate cause and make necessary corrections. This SQS message will not be processed again")

            except Exception as e:
                print(f"WARNING: Error while deleting result file from glacier for glacier job id {job_id_glacier}")
                print(f"Error message: {str(e)}. Error type: {type(e).__name__}")
                print("Please investigate cause and make necessary corrections. This SQS message will not be processed again")

        sqs_batch_response["batchItemFailures"] = batch_item_failures
        return sqs_batch_response
