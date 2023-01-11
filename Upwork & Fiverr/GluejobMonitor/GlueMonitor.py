# import xlsxwriter module
import xlsxwriter
import pandas as pd
import boto3
from datetime import datetime

AWS_Access_Key = "AKIAR5HAKSRJMSP6AW7K"
AWS_Secret_Access_Key = "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"
glue_client = boto3.client('glue', region_name = "eu-west-1", aws_access_key_id = AWS_Access_Key, aws_secret_access_key = AWS_Secret_Access_Key)

#job_name = "etl_salla_novimed"

def gluejobmetrices(job_name):

    succeeded_gluejobdetails = []
    failed_gluejobdetails = []
    response = glue_client.get_job_runs(JobName=job_name)
    for res in response['JobRuns']:
        runstatus = res["JobRunState"]
        if runstatus == "FAILED":
            errormessage = res["ErrorMessage"]
            failed_jobdetails = [job_name, str(res["StartedOn"]), str(res["CompletedOn"]), res["ExecutionTime"], res["JobRunState"], errormessage]
            failed_gluejobdetails.append(failed_jobdetails)
            failed_df = pd.DataFrame(failed_gluejobdetails, columns = ['JOB NAME', 'START TIME', 'END TIME', 'RUN TIME', 'RUN STATUS', 'ERROR MESSAGE'])
        if runstatus == "SUCCEEDED":
            succeeded_jobdetails = ([job_name, str(res["StartedOn"]), str(res["CompletedOn"]), res["ExecutionTime"], res["JobRunState"]])
            succeeded_gluejobdetails.append(succeeded_jobdetails)
            succeeded_df = pd.DataFrame(succeeded_gluejobdetails, columns = ['JOB NAME', 'START TIME', 'END TIME', 'RUN TIME', 'RUN STATUS'])

    print(failed_df)
    print(succeeded_df)


job_name = "etl_salla_novimed"
gluejobmetrices(job_name)




