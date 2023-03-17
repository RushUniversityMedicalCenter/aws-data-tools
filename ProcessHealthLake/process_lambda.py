import requests
import boto3
import os,glob, ast
import json, time
from requests_auth_aws_sigv4 import AWSSigV4
import datetime 
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event,handler):
    
    s3 = boto3.resource('s3')
    source_bucket_name=event['source_bucket']
    error_bucket_name=event['error_bucket']
    final_bucket_name=event['final_bucket']
    file_pickup_folder=event["file_pickup_folder"]
    src_bucket = s3.Bucket(source_bucket_name)
    DATA_STORE_ID=os.environ['DATA_STORE_ID']
    
    error_files_folder_name=event['error_files_folder_name']

    
    my_bucket = s3.Bucket(source_bucket_name)

    #logging to healthlake store
    try:
        base_url = "https://healthlake.us-east-2.amazonaws.com/datastore/{DATA_STORE_ID}/r4".format(DATA_STORE_ID=DATA_STORE_ID)
        ACCESS_ID=os.environ['ACCESS_ID']
        ACCESS_KEY=os.environ['ACCESS_KEY']
        aws_auth = AWSSigV4('healthlake',
                   region='us-east-2',
                   aws_access_key_id=ACCESS_ID,
                   aws_secret_access_key=ACCESS_KEY)
        # aws_auth = AWSSigV4('healthlake',
        #           region='us-east-2',
        #           aws_access_key_id="294739793",
        #           aws_secret_access_key=ACCESS_KEY)
    except:
        logger.info("issue with AWSSigV4")
    
    error_file_400="Json Record::Error Code::Error Reason::timestamp::File Name\n"
    error_file_other_than_400="Json Record::Error Code::Error Reason::timestamp::File Name\n"
    
    for obj in src_bucket.objects.filter(Prefix=file_pickup_folder):
        print("start time->",datetime.datetime.now())
        # try:
        key = obj.key
        logger.info("---starting preprocessing data for file name--- "+str(key))
        if not obj.key.endswith(".ndjson"):
            continue
        
        
        
        lines_raw = obj.get()['Body'].read().decode('utf-8').splitlines()
        lines=list(filter(None,lines_raw))
        no_of_lines_in_file=len(lines)
        
        count = 0
        
        
    # Strips the newline character
        for line in lines:
            
            
            line = line.strip()
            
            payload = json.loads(line)
            resourceType = payload['resourceType'] 
            res_id = payload['id']
            # Ensure FHIR ID is capped at 64 characters
            if len(res_id) > 64:
                res_id = res_id[0:63]
                payload['id'] = res_id
    
            # Read the resource type from the payload and use it in the FHIR URL
            url = base_url +  '/' + resourceType  + '/' + res_id
            
            
            payload_str = json.dumps(payload)
    
            encoded_data = json.dumps(payload_str).encode('utf-8')
            
            response = requests.request("PUT", url, auth=aws_auth, data=payload_str,headers={"Content-Type":"application/json"})
            
            #condition for status code 200
            if response.status_code >= 200 and response.status_code<300:
                
                count += 1
                
            # condition for bad error record
            elif response.status_code ==400:
                current_timestamp = datetime.datetime.now()
                current_timestamp_str=current_timestamp.strftime('%Y-%m-%dT%H:%M:%S') + ('-%02d' % (current_timestamp.microsecond / 10000))
                error_file_400+=line+"::"+str(response.status_code)+"::"+response.reason+"::"+current_timestamp_str+"::"+key+"\n"
            
            # condition for unprocessed record due to API error
            else:
                #API Error
                current_timestamp = datetime.datetime.now()
                current_timestamp_str=current_timestamp.strftime('%Y-%m-%dT%H:%M:%S') + ('-%02d' % (current_timestamp.microsecond / 10000))
                error_file_other_than_400+=line+"::"+str(response.status_code)+"::"+response.reason+"::"+current_timestamp_str+"::"+key+"\n"
                
            
        logger.info("Successful records count is > "+str(count)+", Total record count is -> "+str(no_of_lines_in_file))
        if no_of_lines_in_file==count:
            key_filtered=key.split("/")[-1]
            file_path="NdjsonFiles/"+key_filtered
            transfer_file(source_bucket_name,final_bucket_name,s3,key,file_path)
            logger.info("move files to final bucket in case of everthing fine")
        else:
            year = datetime.datetime.today().year
            month=datetime.datetime.today().month
            day=datetime.datetime.today().day
            key_filtered=key.split("/")[-1]
            file_path="ErrorFiles/"+str(year)+"/"+str(month)+"/"+str(day)+"/"+key_filtered
            transfer_file(source_bucket_name,error_bucket_name,s3,key,file_path)
            logger.info("move files to error bucket in case of errors as line count is less")
        print("end time->",datetime.datetime.now())
    list_error_400=list(filter(None,error_file_400.split("\n")))
    list_error_other_than_400=list(filter(None,error_file_other_than_400.split("\n")))
    
    # error_bad_record file creation
    if len(list_error_400)>1:
        current_timestamp = datetime.datetime.now()
        error_file_name="error_bad_record_"+current_timestamp.strftime('%Y_%m_%dT%H_%M_%S')+".log"
        create_file_in_s3(error_file_400,error_file_name,error_bucket_name,s3,error_files_folder_name)
    
    # error_unprocessed_file_creation
    if len(list_error_other_than_400)>1:
        current_timestamp = datetime.datetime.now()
        error_file_name="error_unprocessed_record_"+current_timestamp.strftime('%Y_%m_%dT%H_%M_%S')+".log"
        create_file_in_s3(error_file_other_than_400,error_file_name,error_bucket_name,s3,error_files_folder_name)
    
        # except:
        #     transfer_file(source_bucket_name,error_bucket_name,s3,key)
        #     logger.info("move file to error bucket in case of exception for filename "+str(key))

def transfer_file(source_bucket_name,destination_bucket_name,s3,key,file_path):
    # try:
    copy_source = {'Bucket': source_bucket_name,'Key':key}
    dest_bucket = s3.Bucket(destination_bucket_name)
    
    
    dest_bucket.copy(copy_source, file_path)
    s3.Object(source_bucket_name,key).delete()
    # except :
    #     logger.info("Incorrect bucket name or some other issue with transferring file")

def create_file_in_s3(data,file_name,bucket_name,s3,error_files_folder_name):
    
    encoded_string = data.encode("utf-8")
    year = datetime.datetime.today().year
    month=datetime.datetime.today().month
    day=datetime.datetime.today().day
    
    s3.Bucket(bucket_name).put_object(Key=error_files_folder_name+str(year)+"/"+str(month)+"/"+str(day)+"/"+file_name, Body=encoded_string)
    logger.info(str(file_name)+ " created" )
    