import requests
import time
import logging
import base64


def setup_oauth_logger(logger):
    global oauth_logger
    oauth_logger = logger


def get_oauth_token(oauth_url, oauth_headers, max_retries):
    retries = 0
    httpTimeoutInterval = 60
    while retries < max_retries:
        try:
            # Make a POST request to the OAuth URL with client credentials
            response = requests.post(
                oauth_url, headers=oauth_headers, timeout=httpTimeoutInterval
            )
            response.raise_for_status()  # Raise an exception for non-2xx status codes

            # Print the OAuth response code.
            oauth_logger.info(
                f"Successful in obtaining OAuth token. Response Status code:{response.status_code}"
            )

            # Extract and return the OAuth token from the response
            token = response.text
            if token:
                return token.strip()
            else:
                oauth_logger.info(
                    f"Failed to obtain OAuth token. Response Status code: {response.status_code}"
                )
                return None
        except requests.exceptions.RequestException as e:
            oauth_logger.info("Error obtaining OAuth token:", e)
            retries += 1
            if retries < max_retries:
                oauth_logger.info(f"Retrying ({retries}/{max_retries}) in 5 seconds...")
                time.sleep(5)
            else:
                oauth_logger.info(f"Maximum retries reached ({max_retries}).")
                return None


# Function to call API
def call_api(api_endpoint, username, password, query_params, api_timeout):
    try:
        # Set headers
        authData = f"{username}:{password}"
        base64AuthData = base64.b64encode(authData.encode()).decode()
        authHeaders = {
            "Authorization": f"Basic {base64AuthData}",
            "Content-Type": "application/json",
        }

        # Log API request
        logging.info(f"API call made to {api_endpoint}")

        response = requests.get(
            api_endpoint,
            params=query_params,
            headers=authHeaders,
            timeout=api_timeout,
            verify=False,
        )

        # Extract json content from the response
        logging.info(
            f"Validate the response and Extract json content and write to file"
        )
        status_code = response.status_code
        if 200 <= status_code < 300:
            logging.info(
                f"API call made to {api_endpoint} completed, Status code: {response.status_code}"
            )
            json_data = response.json()
            return response, True
        else:
            logging.error(
                f"API call to {api_endpoint} failed with status code: {status_code}"
            )
            return None, False
    except requests.exceptions.Timeout as e:
        # Retry for any other error
        logging.error(f"Error in calling Project list API: {e}")
        logging.error(f"Request timed out. Retrying...")
        return None, False  # Indicates other error
    except requests.exceptions.RequestException as e:
        logging.error(f"Error in calling Export API: {e}")
        logging.error(f"Error in calling Export API: {response.content}")
        if response.status_code in [401, 403]:
            # If authentication failure, refresh token and retry
            logging.info(
                f"Received {response.status_code} error. Refreshing token and retrying..."
            )
            return None, False  # Indicates authentication failure
        else:
            # Retry for any other error
            logging.error(f"Received {response.status_code} error. Retrying...")
            return None, False  # Indicates other error
    except Exception as e:
        # Handle all other exceptions
        logging.error(f"An error occurred: {e}")
        return None, False  # Indicates other error
