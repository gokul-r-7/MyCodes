import json

def lambda_handler(event, context):
    # Log the entire event object
    print(f"Received event: {json.dumps(event)}")

    # Get the API Gateway stage variables
    stage_variables = event.get('stageVariables', {})
    
    # Get the full request context
    request_context = event.get('requestContext', {})
    
    # Extract the API ID and stage from the request context
    api_id = request_context.get('apiId', 'Unknown')
    stage = request_context.get('stage', 'Unknown')
    
    # Construct the API URI
    api_uri = f"https://{api_id}.execute-api.{context.invoked_function_arn.split(':')[3]}.amazonaws.com/{stage}"
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': 'REQUEST PROCESSOR LAMBDA SUCCESS',
            'api_uri': api_uri,
            'event': event  # Include the entire event object in the response
        }),
        'headers': {
            'Content-Type': 'application/json'
        }
    }