import json
import boto3
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    # TODO implement
    
    s3 = boto3.resource('s3')
    source_bucket_name=event['source_bucket']
    src_bucket = s3.Bucket(source_bucket_name)
    split_files_folder=event['split_files_folder']
    file_pickup_folder=event['files_pickup_folder']
    archieve_folder_name=event['archieve_folder_name']
    #folder name where files will be splitted
    
    #maximum size of records for one file
    max_record_size=1000
    
    #logic for splitting files iteratively
    for obj in src_bucket.objects.filter(Prefix=file_pickup_folder):
        if not obj.key.endswith(".ndjson"):
            continue
        lines_raw = obj.get()['Body'].read().decode('utf-8').splitlines()
        lines=list(filter(None,lines_raw))
        print("length of lines ",len(lines))
        
        
        if max_record_size>len(lines):
            data="\n".join(lines)
            file_name=str(obj.key).split("/")[-1].replace(".ndjson","")+"-"+str(0)+"_"+str(len(lines))+".ndjson"
            create_file_in_s3(data,file_name,source_bucket_name,s3,split_files_folder)
            continue
        initial_record_size=len(lines)//max_record_size
        count=0
        for _ in range(0,initial_record_size):

            sub_lines=lines[count:count+max_record_size]
            data="\n".join(sub_lines)
            file_name=str(obj.key).split("/")[-1].replace(".ndjson","")+"-"+str(count)+"_"+str(count+max_record_size)+".ndjson"
            
            create_file_in_s3(data,file_name,source_bucket_name,s3,split_files_folder)
            count+=max_record_size
        
        left_lines=lines[count:]
        if len(left_lines)>0:
            data="\n".join(left_lines)
            file_name=str(obj.key).split("/")[-1].replace(".ndjson","")+"-"+str(count)+"_"+str(len(lines))+".ndjson"
            create_file_in_s3(data,file_name,source_bucket_name,s3,split_files_folder)
            
        filename=obj.key
        move_file(s3,source_bucket_name,filename,file_pickup_folder,archieve_folder_name)
        s3.Object(source_bucket_name,filename).delete()
        
        
        print(filename,"deleted")
#creating new file in s3 bucket    
        
def create_file_in_s3(data,file_name,bucket_name,s3,split_files_folder):

    encoded_string = data.encode("utf-8")
    s3.Bucket(bucket_name).put_object(Key=split_files_folder +file_name, Body=encoded_string)
    logger.info(str(file_name)+ " created" )

def move_file(s3,source_bucket_name,file_name,file_pickup_folder,archieve_folder_name):
    copy_source = {
    'Bucket': source_bucket_name,
    'Key': file_name}
    s3.meta.client.copy(copy_source, source_bucket_name, archieve_folder_name+file_name)
    print(file_name," moved to ",archieve_folder_name)
    
        