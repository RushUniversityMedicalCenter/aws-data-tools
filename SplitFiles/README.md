# Lambda: SplitFiles-rush

## Input Parameters:
1. source_bucket - 
2. split_files_folder - folder to which files will be placed after splitting
3. files_pickup_folder - actual files fetching folder

#Process
1. files are picked from processData-dev bucket from NdjsonFiles folder iteratively
2. Each file is passed through a logic as below
	1. if record count in a file is greater than 1000 , a new file with name 
	FILENAME_INITIAL_RECORD_COUNT_INITIAL_RECORD_COUNT+1000.ndjson is saved i.e patient_history_1001-2001.ndjson
	2. Subsequent files are created as per record count of actual filename
	3. if record size is less than 1000 , then one file is created and saved as filename_0_RECORD_COUNT.NDJSON
3. Actual file is deleted once all the steps are performed