# Lambda 2:ProcessData

## Input Parameters:
 1. "source_bucket" : bucket from which files will be fetched
 2. "error_bucket" : placing error logs 
 3. "final_bucket" : in case of success , files should be placed here
 4. "file_pickup_folder" : files pick up folder for ndjson file
## Process
 1. Logging to healthlake store
 2. Fetching files from split folder
	1. Saving each record to healthlake
	2. Incase of any error
		1. Status code 200 - data saved to data store
		2. Status code 400 - new error file is created with filename - error_bad_record_CURRENT_TIMESTAMP.log
		3. Status code other than 400 - new error file is created with filename - error_unprocessed_CURRENT_TIMESTAMP.log
 3. All the error files are saved in the error_bucket and file structure is followed as below
"ErrorFiles/"+str(year)+"/"+str(month)+"/"+str(day)+"filename_CURRENT_TIMESTAMP".log

