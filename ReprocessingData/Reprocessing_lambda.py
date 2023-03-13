import requests
import boto3
import os, glob, ast
import json, time
from requests_auth_aws_sigv4 import AWSSigV4
import datetime
import logging
import pandas as pd
import numpy as np
from io import StringIO

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, handler):
    s3 = boto3.resource('s3')
    source_bucket_name = event['source_bucket']
    error_bucket_name = event['error_bucket']
    final_bucket_name = event['final_bucket']
    error_folder_name_inside_bucket = event['error_folder_name_inside_bucket']
    src_bucket = s3.Bucket(error_bucket_name)
    DATA_STORE_ID = os.environ['DATA_STORE_ID']

    # logging to data healthlake
    try:
        base_url = "https://healthlake.us-east-2.amazonaws.com/datastore/{DATA_STORE_ID}/r4".format(
            DATA_STORE_ID=DATA_STORE_ID)
        ACCESS_ID = os.environ['ACCESS_ID']
        ACCESS_KEY = os.environ['ACCESS_KEY']
        aws_auth = AWSSigV4('healthlake',
                            region='us-east-2',
                            aws_access_key_id=ACCESS_ID,
                            aws_secret_access_key=ACCESS_KEY)
    except:
        logger.info("issue with AWSSigV4")

    # columns of files feteched from error_bucket
    columns = ['Json Record', 'Error Code', 'Error Reason', 'timestamp', 'File Name']

    df_updated = pd.DataFrame(columns=columns)

    year = datetime.datetime.today().year
    month = datetime.datetime.today().month
    day = datetime.datetime.today().day

    # iterating through error files
    for obj in src_bucket.objects.filter(Prefix="ErrorLogFiles/" + str(year) + "/" + str(month) + "/" + str(day) + "/"):

        success_list = [columns]
        error400_list = [columns]
        error_other_than_400_list = [columns]

        # try:
        key = obj.key

        if not obj.key.endswith(".log"):
            continue

        logger.info("---starting preprocessing data for file name--- " + str(key))

        lines_raw = obj.get()['Body'].read().decode('utf-8')

        df = pd.read_csv(StringIO(lines_raw), sep="::")

        print(df.columns)
        print(df.head())
        for i in df.index:

            try:
                line = df['Json Record'].iloc[i]
            except:
                print("line", df.iloc[i])
            payload = json.loads(line)
            resourceType = payload['resourceType']
            res_id = payload['id']
            # Ensure FHIR ID is capped at 64 characters
            if len(res_id) > 64:
                res_id = res_id[0:63]
                payload['id'] = res_id

            # Read the resource type from the payload and use it in the FHIR URL
            url = base_url + '/' + resourceType + '/' + res_id

            payload_str = json.dumps(payload)

            encoded_data = json.dumps(payload_str).encode('utf-8')

            # request to put data to healthlake
            response = requests.request("PUT", url, auth=aws_auth, data=payload_str,
                                        headers={"Content-Type": "application/json"})

            # condition when status code is 200
            if response.status_code >= 200 and response.status_code < 300:
                current_timestamp = datetime.datetime.now()
                current_timestamp_str = current_timestamp.strftime('%Y-%m-%dT%H:%M:%S') + (
                            '-%02d' % (current_timestamp.microsecond / 10000))

                success_list.append(
                    [line, str(response.status_code), response.reason, current_timestamp_str, df['File Name'].iloc[i]])

            # condtion for bad request
            elif response.status_code == 400:
                #         count += 1
                current_timestamp = datetime.datetime.now()
                current_timestamp_str = current_timestamp.strftime('%Y-%m-%dT%H:%M:%S') + (
                            '-%02d' % (current_timestamp.microsecond / 10000))

                error400_list.append(
                    [line, str(response.status_code), response.reason, current_timestamp_str, df['File Name'].iloc[i]])
            # condition for unprocessed eror
            else:
                current_timestamp = datetime.datetime.now()
                current_timestamp_str = current_timestamp.strftime('%Y-%m-%dT%H:%M:%S') + (
                            '-%02d' % (current_timestamp.microsecond / 10000))

                error_other_than_400_list.append(
                    [line, str(response.status_code), response.reason, current_timestamp_str, df['File Name'].iloc[i]])

        # incase of unprocessed error it will create a new file and save all records and file is named as _attempted_file
        if len(error_other_than_400_list) > 1:
            data_list = ["::".join(i) for i in error_other_than_400_list]
            data_string = "\n".join(data_list)
            current_timestamp = datetime.datetime.now()
            current_timestamp_str = current_timestamp.strftime('%Y-%m-%dT%H:%M:%S') + (
                        '-%02d' % (current_timestamp.microsecond / 10000))
            create_file_in_s3(data_string, key.replace(".log", "") + "_attempted_at_" + current_timestamp_str + ".log",
                              error_bucket_name, s3)

        if len(success_list) > 1:
            logger.info("successfull response records are " + str(len(success_list)))
        if len(error400_list) > 1:
            logger.info("bad response records are " + str(len(error400_list)))

        # once everything is done ..delete the error file
        delete_error_log_file(error_bucket_name, s3, key)


# this function helpus to create   file in s3 bucket
def create_file_in_s3(data, file_name, bucket_name, s3):
    encoded_string = data.encode("utf-8")

    s3.Bucket(bucket_name).put_object(Key=file_name, Body=encoded_string)
    logger.info(str(file_name) + " created")


# this function helps to delete file from s3 bucket
def delete_error_log_file(error_bucket_name, s3, key):
    s3.Object(error_bucket_name, key).delete()
    logger.info(str(key) + " deleted")














