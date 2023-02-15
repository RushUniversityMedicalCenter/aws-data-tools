import requests
import boto3
import os,glob, ast
import json, time
from requests_auth_aws_sigv4 import AWSSigV4
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event,handler):
    
    s3 = boto3.resource('s3')
    source_bucket_name=event['source_bucket']
    error_bucket_name=event['error_bucket']
    final_bucket_name=event['final_bucket']
    
    src_bucket = s3.Bucket(source_bucket_name)
    DATA_STORE_ID=os.environ['DATA_STORE_ID']
    try:
        base_url = "https://healthlake.us-east-2.amazonaws.com/datastore/{DATA_STORE_ID}/r4".format(DATA_STORE_ID=DATA_STORE_ID)
        ACCESS_ID=os.environ['ACCESS_ID']
        ACCESS_KEY=os.environ['ACCESS_KEY']
        aws_auth = AWSSigV4('healthlake',
                   region='us-east-2',
                   aws_access_key_id=ACCESS_ID,
                   aws_secret_access_key=ACCESS_KEY)
    except:
        logger.info("issue with AWSSigV4")
    
    for obj in src_bucket.objects.all():
        
        try:
            key = obj.key
            logger.info("---starting preprocessing data for file name--- "+str(key))
            
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
                
                if response.status_code == 200:
                    count += 1
                    
                
                else:
                    logger.info("line "+str(line))
                    logger.info("count number "+str(count))
                    logger.info("playload string -> ",str(payload_str))
                    logger.info("Response from HealthLake="+str(response.status_code))
                    logger.info("Response from HealthLake="+response.reason)
                
            logger.info("Count of current index and count of lines in file "+str(count)+" "+str(no_of_lines_in_file))
            if no_of_lines_in_file==count:
                
                transfer_file(source_bucket_name,final_bucket_name,s3,key)
                logger.info("move files to final bucket in case of everthing fine")
            else:
                
                transfer_file(source_bucket_name,error_bucket_name,s3,key)
                logger.info("move files to error bucket in case of errors as line count is less")
                
            
            
        except:
            transfer_file(source_bucket_name,error_bucket_name,s3,key)
            logger.info("move file to error bucket in case of exception for filename "+str(key))

def transfer_file(source_bucket_name,destination_bucket_name,s3,key):
    # try:
    copy_source = {'Bucket': source_bucket_name,'Key': key}
    dest_bucket = s3.Bucket(destination_bucket_name)
    dest_bucket.copy(copy_source, key)
    s3.Object(source_bucket_name,key).delete()
    # except :
        # logger.info("Incorrect bucket name or some other issue with transferring file")
    