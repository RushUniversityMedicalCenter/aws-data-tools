# Lambda : FileMove

# Input Parameters 
  1. "source": source bucket
  2. "destination": destination bucket

# Process
1. Created a S3 Trigger i.e if a file is added to source bucket , lambda will be executed
2. Create a environment variable i.e bucket_destination which has the path of file transferred
3. Moving files from source to a particular folder in another bucket 


