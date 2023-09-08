#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
#
# Wrapper script for running AnnTools
#
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import sys
import time
import driver
import shutil
import os
import boto3
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from boto3.exceptions import S3UploadFailedError
import json

# Get configuration
from configparser import ConfigParser, ExtendedInterpolation
config = ConfigParser(os.environ, interpolation=ExtendedInterpolation())
config.read("annotator_config.ini")

# S3 bucket name for annotation results
gas_result_bucket = config.get('s3', 'ResultsBucketName')

aws_region = config.get('aws', 'AwsRegionName')

# DynamoDB table name
dynamo_tbl_name = config.get('gas', 'AnnotationsTable')

# sns topic name for job results publication
sns_job_results_topic_arn = config.get('sns', 'JobResultsTopicArn')



#-------------------------------------------------------------------------------
# Auxiliary functions
#-------------------------------------------------------------------------------

def remove_files(file_list):
    for f in file_list:
        try:
            os.remove(f)
        except OSError as e:
            print(f"Error: could not remove file. OSError message: {str(e)}. Error type: {type(e).__name__}")
        except Exception as e:
            print(f"Unexpected Error: could not remove file. Error message: {str(e)}. Error type: {type(e).__name__}")

def handle_updload_error(error_msg, files_to_remove):
    try:
        with open(f"{os.path.dirname(sys.argv[1])}/error_log.txt", "w") as f:
            f.write(error_msg)

        print(error_msg)
        print("An error log has been saved to the job's directory.")
        remove_files(files_to_remove)
        sys.exit(0)

    except OSError as e:
        print(f"OSError: could not write error log to file. Error message: {str(e)}. Error type: {type(e).__name__}")

    except Exception as e:
        print(f"Unexpected Error: could not write error log to file. Error message: {str(e)}. Error type: {type(e).__name__}")

"""A rudimentary timer for coarse-grained profiling
"""
class Timer(object):
  def __init__(self, verbose=True):
    self.verbose = verbose

  def __enter__(self):
    self.start = time.time()
    return self

  def __exit__(self, *args):
    self.end = time.time()
    self.secs = self.end - self.start
    if self.verbose:
      print(f"Approximate runtime: {self.secs:.2f} seconds")

#-------------------------------------------------------------------------------
# Main
#-------------------------------------------------------------------------------

if __name__ == '__main__':
    # Call the AnnTools pipeline
    if len(sys.argv) == 6:
        with Timer():
            driver.run(sys.argv[1], 'vcf')
        # time the job was completed (point of view of the annotator)
        # obs: int(time.time()) to follow Professor convention
        complete_time = int(time.time())
        # get the job ID from command line inputs
        job_id = sys.argv[2]

        # get the prefix for S3 key from command line inputs
        # eg: mashalimay/userX/
        prefix = sys.argv[3]

        # get the user's email from command line inputs; 
        # email = sys.argv[4];

        # get the user's ID from command line inputs
        user_id = sys.argv[4]

        role = sys.argv[5]

        
        #-----------------------------------------------------------------------
        # Create paths to the output files
        #-----------------------------------------------------------------------
        
        # full path to the annotated file in local machine
          # eg: /home/ubuntu/jobs/<job_id>/<job_id>~test.vcf.annot.vcf 
          # -> /home/ubuntu/jobs/<job_id>/<job_id>~test.annot.vcf
        annot_file = sys.argv[1].split(".vcf")[0] + ".annot.vcf"

        # full path to the log file in local machine
          # eg: /home/ubuntu/jobs/<job_id>/<job_id>~test.vcf.annot.vcf 
          # -> /home/ubuntu/jobs/<job_id>/<job_id>~test.vcf.count.log
        log_file = sys.argv[1] + ".count" + ".log"

        # create S3 keys for the annotated and log files
        s3_key_result_file = prefix + os.path.basename(annot_file)
        s3_key_log_file = prefix + os.path.basename(log_file)

        #-----------------------------------------------------------------------
        # upload annotated and log file to S3
        #-----------------------------------------------------------------------
        
        s3 = boto3.client("s3", config=Config(signature_version='s3v4'), region_name=aws_region)

        try:
            s3.upload_file(annot_file, gas_result_bucket, s3_key_result_file)
            s3.upload_file(log_file, gas_result_bucket, s3_key_log_file)
            
        except S3UploadFailedError as e:
            error_msg = f"Failure to upload result and log files to S3 bucket. \n" \
                        f"Local files, including input file, have been removed from EBS volume to save space. \n" \
                        f"Please investigate the error messages and re-do the annotation job. \n" \
                        f"Error message generated: {str(e)}\n" \
                        f"File for which the error ocurred: {os.path.basename(sys.argv[1])} "
            handle_updload_error(error_msg, [annot_file, log_file, sys.argv[1]])            
        
        except ClientError as e:
            # if no specific error code, provide information that can be helpful
            # following guidelines in https://boto3.amazonaws.com/v1/documentation/api/latest/guide/error-handling.html
            error_msg = f"Failure to upload result and log files to S3 bucket. \n" \
                        f"Local files, including input file, have been removed from EBS volume to save space. \n" \
                        f"Please investigate the error messages and re-do the annotation job. \n" \
                        f"Error message generated: {e.response['Error']['Message']}\n" \
                        f"Request ID: {e.response['ResponseMetadata']['RequestId']}\n" \
                        f"HTTP code: {e.response['ResponseMetadata']['HTTPStatusCode']}\n" \
                        f"File for which the error ocurred: {os.path.basename(sys.argv[1])}"
            handle_updload_error(error_msg, [annot_file, log_file, sys.argv[1]])

        except Exception as e:
            error_msg = f"Failure to upload result and log files to S3 bucket. \n" \
                        f"Local files, including input file, have been removed from EBS volume to save space. \n" \
                        f"Please investigate the error messages and re-do the annotation job. \n" \
                        f"Error message generated: {str(e)}. Error type: {type(e).__name__}\n" 
            handle_updload_error(error_msg, [annot_file, log_file, sys.argv[1]])

        #-----------------------------------------------------------------------
        # No upload errors => remove job directory from EC2 and update database
        #-----------------------------------------------------------------------
        else:
            # delete the job directory and files
            try:
                shutil.rmtree(os.path.dirname(sys.argv[1]))
            except OSError as e:
                print(f"OSError: could not delete directory {os.path.dirname(sys.argv[1])} from EC2 instance. Error message: {str(e)}. Error type: {type(e).__name__}")
            except Exception as e:
                print(f"Unexpected Error: could not delete directory {os.path.dirname(sys.argv[1])} from EC2 instance. Error message: {str(e)}. Error type: {type(e).__name__}")

            # update the DynamoDB database
            # Obs: only update the job status if the current status is 'running'
            dynamodb = boto3.client('dynamodb', config=Config(signature_version='s3v4'), region_name=aws_region)
            
            try:
                response = dynamodb.update_item(
                    TableName=dynamo_tbl_name,
                    Key={'job_id': {'S': job_id}},
                    UpdateExpression=
                        'SET #s3_results_bucket = :s3_results_bucket, ' +
                        '#s3_key_result_file = :s3_key_result_file, ' +
                        '#s3_key_log_file = :s3_key_log_file, ' +
                        '#complete_time = :complete_time, ' +
                        '#job_status = :new_status',

                    ConditionExpression='#job_status = :old_status',
                    
                    ExpressionAttributeNames={
                        '#s3_results_bucket': 's3_results_bucket',
                        '#s3_key_result_file': 's3_key_result_file',
                        '#s3_key_log_file': 's3_key_log_file',
                        '#complete_time': 'complete_time',
                        '#job_status': 'job_status'
                    },
                    ExpressionAttributeValues={
                        ':s3_results_bucket': {'S': gas_result_bucket},
                        ':s3_key_result_file': {'S': s3_key_result_file},
                        ':s3_key_log_file': {'S': s3_key_log_file},
                        ':complete_time': {'N': str(complete_time)},
                        ':new_status': {'S': 'COMPLETE'},
                        ':old_status': {'S': 'RUNNING'}
                    },
            )        
            except ClientError as e:
                print(f"WARNING: annotation job {job_id} completed successfuly, but DynamoDB could not be updated." +\
                        f"AWS Error message: {e.response['Error']['Message']}. " +
                        f"AWS Request ID: {e.response['ResponseMetadata']['RequestId']}. " +
                        f"AWS HTTP Status Code: {e.response['ResponseMetadata']['HTTPStatusCode']}")

            # publish notification to SNS topic if both upload and database update were successful
            else:
                try:
                    sns_client = boto3.client('sns', config=Config(signature_version='s3v4'), region_name=aws_region)

                    # Publish the message to the SNS topic
                    response = sns_client.publish(
                        TopicArn = sns_job_results_topic_arn,
                        Message = json.dumps({
                            'user_id': user_id,
                            'job_id': job_id,
                            's3_results_bucket': gas_result_bucket,
                            's3_key_result_file': s3_key_result_file,
                            's3_key_log_file': s3_key_log_file,
                            'complete_time': complete_time,
                            'role': role
                            }
                        )
                    )
                    print(f"Notification to SNS job results topic published successfully for job {job_id}.")
                except ClientError as e:
                    print(f"WARNING: annotation job {job_id} completed successfuly, \
                        but notification to SNS job results topic could not be published." +\
                        f"AWS Error message: {e.response['Error']['Message']}. " +
                        f"AWS Request ID: {e.response['ResponseMetadata']['RequestId']}. " +
                        f"AWS HTTP Status Code: {e.response['ResponseMetadata']['HTTPStatusCode']}")

                except Exception as e:
                    print(f"WARNING: annotation job {job_id} completed successfuly,\
                        but notification to SNS job results topic could not be published." +\
                        f"Error message: {str(e)}. Error type: {type(e).__name__}")
        
   
    else:
        print("usage: python3 annotate.py <annotated_vcf_file> <job_id> <prefix_results_bucket> <user_id> <role>")






### EOF