import json
import boto3
from urllib import response
import boto3

AWS_Access_Key = "AKIAU5AFY726E3GCCDBS"
AWS_Secret_Access_Key = "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"
SQS_QUEUE_NAME = "DataGrokr_Queue"

#Reading a USER JSON file
with open('user.json') as user_json_file:
    read_json_data = user_json_file.read()

#Parsing the Data from a USER JSON file

    json_data = json.loads(read_json_data)

    #for users in json_data['Users:']:
    #    print(users)
        
class SQSQueue(object):
    def __init__(self, Queue_Name = None):
        self.resource = boto3.resource('sqs', region_name = "us-west-2", aws_access_key_id = AWS_Access_Key, aws_secret_access_key = AWS_Secret_Access_Key)
        self.queue = self.resource.get_queue_by_name(QueueName = SQS_QUEUE_NAME)
        self.QueueName = Queue_Name

    def send_message(self, Message = {}):
        data = json.dumps(Message)
        response = self.queue.send_message(MessageBody = data)
        return response
if __name__ == "__main__":
    queue = SQSQueue(Queue_Name = SQS_QUEUE_NAME)
    
    for users in json_data['Users:']:
        queue.send_message(Message = users)        