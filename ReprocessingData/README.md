# Reprocessingdata-rush

# Input parameters
1. "source_bucket": source bucket name,
2. "error_bucket": error bucket name,
3. "final_bucket": final successfull filesuploaded bucket name,
4. "error_folder_name_inside_bucket":"ErrorLogFiles"


## Process
1. All the error files of that particular day are picked . Folder structure is as below
"ErrorLogFiles/"+str(year)+"/"+str(month)+"/"+str(day)+"/")
2. All the records in error file is accessed iteratively . Below are the conditions on which data is saved
	1. if status code is 200, then count of successfull records transferred to healthlake is saved
	2. if status code is 400 , which denotes bad request , these data points are left alone
	3. if status code is other than 400 which denotes record is unprocessed due to some issues at API level.These records
	are saved in another file and file is named as OLD_fILENAME__attempted_at_CURRENT_TIMESTAMP.log
3.Finally the error file is deleted after following all the steps
