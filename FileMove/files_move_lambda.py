import json
import boto3
import logging
import os
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    # TODO implement
    records = [x for x in event.get('Records', []) if x.get('eventName') == 'ObjectCreated:Put']
    logger.info("records-"+str(records))
    
    
    if len(records)!=0:
        s3 = boto3.resource('s3')
        
        source_bucket_name=records[0]['s3']['bucket']['name']
        
        destination_bucket_name=os.environ['BUCKET_DESTINATION']
        destination_folder_name=os.environ['DESTINATION_FOLDER_NAME']
        
        src_bucket = s3.Bucket(source_bucket_name)
    
        dest_bucket = s3.Bucket(destination_bucket_name)
        filename=records[0]['s3']['object']['key']
        logger.info(records)
        
        copy_source = {'Bucket': source_bucket_name,'Key': filename}
        dest_bucket.copy(copy_source, destination_folder_name + filename)
        s3.Object(source_bucket_name,filename).delete()
        
        
        logger.info(filename +'- File Moved')
   
    
