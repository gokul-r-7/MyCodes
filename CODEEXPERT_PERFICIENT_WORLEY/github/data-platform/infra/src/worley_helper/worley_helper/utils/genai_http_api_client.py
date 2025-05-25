import requests
import json
import time
from worley_helper.utils.logger import get_logger

# Init the logger
logger = get_logger(__name__)
class HTTPClient:
    def __init__(self, config_yaml):
        self.config = config_yaml
        self.auth_token = None

        """ # Add default values for missing attributes
        self.config.setdefault('auth_headers', {})
        self.config.setdefault('api_headers', {})
        self.config.setdefault('api_params', {})
        self.config.setdefault('api_data', {}) """

    def make_api_request(self, method, url, params=None, data=None, headers=None, timeout=600, ssl_verify=True, max_retries=None):
        SUCCESS = True
        FAILURE = False

        if timeout is None:
            timeout = self.config.get('api_parameter')['api_timeout']
        
        if 'max_retries' in self.config.get('api_parameter'):
            max_retries = self.config.get('api_parameter')['max_retries']

        if max_retries is None or max_retries < 1:
            max_retries = 1
        
        api_wait_sec = None
        if 'api_wait_sec' in self.config.get('api_parameter'):
            api_wait_sec = self.config.get('api_parameter')['api_wait_sec']

        if api_wait_sec is None or (api_wait_sec < 5 or api_wait_sec > 15):# max for retry is 15 sec
            api_wait_sec = 5
        retries = 0
        while retries < max_retries:
            try:
                logger.info(f"Making {method} request to {url}")

                if method == "get":
                    # It appears passing body to GET, even if its empty, causes the response object to have unpredectable results (e.g: truncated values)
                    logger.info(f"Parms  {params} Headers {headers} Verify {ssl_verify}")
                    response = requests.request(method, url, params=params,
                                            headers=headers, timeout=timeout)
                else:
                    response = requests.request(method, url, params=params, data=data,
                                            headers=headers, timeout=timeout, verify=ssl_verify)
                response.raise_for_status()
                logger.info(f"Success: {response.status_code} - {method} request to {url}")
                return response, SUCCESS, response.status_code

            except requests.exceptions.RequestException as e:
                logger.error(f"Error making API request: {e}")
                retries += 1
                if retries < max_retries:
                    logger.info(f"Retrying ({retries}/{max_retries}) in {api_wait_sec} seconds.. for request to {url}")
                    time.sleep(api_wait_sec)
                else:
                    logger.info(
                        f"Maximum retries reached ({max_retries}) for request to {url}.")
                    return None, FAILURE, None


    def extract_content(
            self,
            response,
            response_type,
            status_code,
            is_auth_response=False
            ):
        SUCCESS = True
        FAILURE = False
        status_code = status_code
        try:
            content_extractors = {
                'json': lambda: response.json().get("access_token") if is_auth_response else response.json(),
                'binary': lambda: response.content,
                'xml': lambda: response.content,
                'text': lambda: response.text.strip() if is_auth_response else response.text,
                'raw': lambda: response
            }
            
            content_extractor = content_extractors.get(response_type, response.json)
            return content_extractor(), SUCCESS, status_code
        except Exception as e:
            logger.info(f"Error extracting content: {e}")
            return None, FAILURE, status_code


    def authenticate(self):
        SUCCESS = True
        FAILURE = False
        try:
            auth_info = self.config.get('auth_api_parameter')

            if not auth_info:
                logger.info("Authentication info not found. Skipping authentication.")
                return SUCCESS

            auth_api = auth_info.get('endpoint')
            auth_method = auth_info.get('auth_method')
            auth_params = auth_info.get('auth_query_params')
            auth_data = auth_info.get('auth_body')
            auth_headers = auth_info.get('auth_headers')
            auth_timeout = auth_info.get('auth_timeout', 600)
            auth_ssl_verify = auth_info.get('auth_ssl_verify', True)
            api_response_type = auth_info.get('api_response_type', 'json')

            params = {k: v for k, v in (auth_params or {}).items() if v}
            data = auth_data if auth_data else None
            headers = {k: v for k, v in (auth_headers or {}).items() if v}

            response, status, status_code = self.make_api_request(
                auth_method,
                auth_api,
                params=params,
                data=data,
                headers=headers,
                timeout=auth_timeout,
                ssl_verify=auth_ssl_verify
                )
            
            logger.info(f"Response status: {status}")
            if response and response.status_code == 200:
                token, status, status_code = self.extract_content(
                    response,
                    api_response_type,
                    status_code,
                    is_auth_response=True
                    )
                if token:
                    self.auth_token = token
                    logger.info("Authentication successful. Token obtained.")
                    return SUCCESS

            logger.error("Authentication failed. Exiting.")
            return FAILURE
        except Exception as e:
            logger.error(f"Error during authentication: {e}")
            return FAILURE


    def run(self):
        SUCCESS = True
        FAILURE = False
        try:
            if not self.authenticate():
                return None, FAILURE, None

            api_info = self.config.get('api_parameter')
            logger.info(f"API info: {api_info}")
            api_endpoint = api_info.get('endpoint')
            api_method = api_info.get('api_method')
            api_params = api_info.get('api_query_params')
            api_data = api_info.get('api_body')
            api_headers = api_info.get('api_headers')
            api_timeout = api_info.get('api_timeout', 600)
            api_ssl_verify = api_info.get('api_ssl_verify', True)
            api_response_type = api_info.get('api_response_type', 'json')

            params = {k: v for k, v in (api_params or {}).items() if v}
            data = api_data if api_data else None
            headers = {k: v for k, v in (api_headers or {}).items() if v}

            if self.auth_token:
                headers['Authorization'] = f'Bearer {self.auth_token}'

            if not all((api_endpoint, api_method)):
                logger.error("Missing required API attributes. Exiting.")
                return None, FAILURE, None
            
            logger.info(f"API header info:{headers} ")

            response, status, status_code = self.make_api_request(
                api_method,
                api_endpoint,
                params=params,
                data=json.dumps(data),
                headers=headers,
                timeout=api_timeout,
                ssl_verify=api_ssl_verify
                )
            
            if response is None:
                logger.error("API request failed. Exiting.")
                return None, status, status_code

            content, status, status_code = self.extract_content(
                response,
                api_response_type,
                status_code
                )
            
            if status == FAILURE:
                logger.error("Error extracting content. Exiting.")
                return None, SUCCESS, status_code  #Return SUCCESS as the HTTP API itself returned 200 OK, but an internal logic failed.

            return content, SUCCESS, status_code

        except Exception as e:
            logger.error(f"Error during run: {e}")
            return None, FAILURE, None