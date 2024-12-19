import sys
import boto3
import logging

# Setting up the CloudWatch Logs client
log_group_name = '/aws/glue/jobs'
log_stream_name = 'your-log-stream'  # Replace with your log stream name

client = boto3.client('logs')

# Create CloudWatch Log Stream if it doesn't exist
def create_log_stream():
    try:
        response = client.create_log_stream(
            logGroupName=log_group_name,
            logStreamName=log_stream_name
        )
        print("Log stream created successfully.")
    except client.exceptions.ResourceAlreadyExistsException:
        print("Log stream already exists.")
    except Exception as e:
        print(f"Error creating log stream: {e}")

# Function to put log events
def write_log_to_cloudwatch(message):
    timestamp = int(round(time.time() * 1000))  # Current timestamp in milliseconds
    try:
        response = client.put_log_events(
            logGroupName=log_group_name,
            logStreamName=log_stream_name,
            logEvents=[
                {
                    'timestamp': timestamp,
                    'message': message
                }
            ]
        )
        print("Log event sent to CloudWatch")
    except Exception as e:
        print(f"Error sending log event to CloudWatch: {e}")

# Set up logging configuration
def setup_logging():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    logger.addHandler(handler)
    return logger

def main():
    # Initialize the logger
    logger = setup_logging()

    # Optionally, create a log stream if not exists
    create_log_stream()

    # Example logs
    logger.info("This is an info log message")
    write_log_to_cloudwatch("This is a log message sent to CloudWatch")

    # Simulate some Glue job processing
    try:
        # Your Glue job logic here
        logger.info("Glue job is starting...")
        write_log_to_cloudwatch("Glue job started")
        
        # Simulating some job steps
        logger.info("Performing data transformation...")
        write_log_to_cloudwatch("Data transformation step completed")

        # More job steps...
        logger.info("Job completed successfully")
        write_log_to_cloudwatch("Job completed successfully")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        write_log_to_cloudwatch(f"Error: {e}")

if __name__ == '__main__':
    main()
