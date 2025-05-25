from helpers import utilities,sns

utils = utilities()
logger = utils.get_logger()
settings = utils.get_settings()

def lambda_handler(event,context):
    try:
        failed_dag = event['dag_name']
        sns.notify().send(topic_arn=settings['data_modelling_notifications_arn'], 
                        subject="Data modelling dags - WARNING", 
                        message=f"Data modelling DAG - {failed_dag} has failed, please inform the data modelling team")
    except KeyError as e:
        logger.error(f"KeyError: invalid payload - dag_name not present")
        
if __name__ == "__main__":
    lambda_handler("","")