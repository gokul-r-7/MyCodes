import requests
import json
from worley_helper.utils.logger import get_logger
from worley_helper.utils.aws import get_secret, S3, DynamoDB

# Init the logger
logger = get_logger(__name__)
class HTTPClient:
    def __init__(self, config_yaml):
        self.config = config_yaml
        self.auth_token = None
        self.token_reuse = None

        """ # Add default values for missing attributes
        self.config.setdefault('auth_headers', {})
        self.config.setdefault('api_headers', {})
        self.config.setdefault('api_params', {})
        self.config.setdefault('api_data', {}) """

    def make_api_request(self, method, url, params=None, data=None, headers=None, timeout=600, ssl_verify=True):
        SUCCESS = True
        FAILURE = False

        if timeout is None:
            timeout = self.config.get('api_parameter')['api_timeout']
        try:
            logger.info(f"Making {method} request to {url}")

            if method == "get":
                #response = requests.get(url, params=params, headers=headers, timeout=timeout)
                # It appears passing body to GET, even if its empty, causes the response object to have unpredectable results (e.g: truncated values)
                logger.info(f"Parms  {params} Headers {headers} Verify {ssl_verify}")
                response = requests.request(method, url, params=params,
                                        headers=headers, timeout=timeout, verify=ssl_verify)
            else:
                response = requests.request(method, url, params=params, data=data,
                                        headers=headers, timeout=timeout, verify=ssl_verify)
            response.raise_for_status()
            logger.info(f"Success: {response.status_code} - {method} request to {url}")
            return response, SUCCESS, response.status_code

        except requests.exceptions.RequestException as e:
            if isinstance(e, requests.exceptions.HTTPError):
                if e.response.status_code == 403:
                    logger.error(f"Error making API request: {e}")
                    return e.response, FAILURE , e.response.status_code
                elif e.response.status_code == 401:
                    logger.error(f"Error making API request: {e}")
                    return e.response, FAILURE , e.response.status_code
                else:
                    logger.error(f"Error making API request: {e}")
                    auth_info = self.config.get('auth_api_parameter', {})
                    app_name = auth_info.get('api_name','main')
                    if app_name.lower() == "erm_min_ril":
                        return e.response, FAILURE, e.response.status_code
                    return None, FAILURE , e.response.status_code
            else:
                logger.error(f"Error making API request: {e}")
                return None, FAILURE , e.response.status_code


    def authenticate(self):
        SUCCESS = True
        FAILURE = False
        try:
            auth_info = self.config.get('auth_api_parameter')
            auth_info = auth_info if auth_info is not None else {}
            api_info = self.config.get('api_parameter')
            job_info = self.config.get('job_parameter')
            
            api_bucket_name = job_info.get('bucket_name')
            kms_key_id = job_info.get('kms_key_id')
            region = self.config.get('aws_region')
            
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
            auth_api_name = auth_info.get('api_name','main') 
            print("auth_api_name --> " + auth_api_name)
            
            s3_client = S3(api_bucket_name, region)

            params = {k: v for k, v in (auth_params or {}).items() if v}
            data = auth_data if auth_data else None
            headers = {k: v for k, v in (auth_headers or {}).items() if v}
            
            if auth_api_name.lower() == "erm_min_ril":

                response, status, status_code = self.make_api_request(auth_method, auth_api, params=params, data=data, headers=headers, timeout=auth_timeout, ssl_verify=auth_ssl_verify)
                logger.info(f"Response status: {status}")
                if response and status_code == 200:
                    json_data = response.json()
                    token = json_data.get("access_token")
                    if token:
                        logger.info(f"Updating the new token to ")
                        self.auth_token = token
                        return SUCCESS
                    else:
                        logger.error(f"Failed to fetch ERM Auth Token as access_token is not found in response. Response - {response=}")
                        return FAILURE
                else:
                    logger.error(f"Failed to fetch ERM Auth Token. Response - {response=}")
                    return FAILURE

            elif auth_api_name.lower() == "erm":

                object_name = "supply_chain/erm/erm_token/erm_token.txt"

                folder_name="supply_chain/erm/erm_token"
                file_name="erm_token.txt"
                content,status=s3_client.read_s3_file(folder_name,file_name)
                
                print(status)
                print(content)
                if content:
                    token=content
                    print("token --> " + token)
                    if token:
                      self.auth_token = token
                      logger.info("Authentication successful. Token obtained.")
                      return SUCCESS
                else:
                  response, status, status_code = self.make_api_request(auth_method, auth_api, params=params, data=data, headers=headers, timeout=auth_timeout, ssl_verify=auth_ssl_verify)
                  logger.info(f"Response status: {status}")
                  if response and response.status_code == 200:
                      if auth_api_name.lower() == "erm" or auth_api_name.lower() == "erm_min_ril":
                          print(response)
                          json_data = response.json()
                          token = json_data.get("access_token")
                          if token:
                            if s3_client.upload_to_s3(token,object_name,kms_key_id,is_gzip=False):
                              logger.info(f"Uploaded ERM Token to {api_bucket_name}/{object_name}")
                              success = True
                              self.auth_token = token
                              return SUCCESS
                            else:
                              logger.error("ERM Token upload to S3 failed")
                              success = False
                              return FAILURE
                          else:
                            logger.error("Failed to fetch Aconex Workflow Export API payload")
                            success = False   
                            return FAILURE                  
                      else:
                          token = response.text.strip()
                          if token:
                            self.auth_token = token
                            logger.info("Authentication successful. Token obtained. ")
                            return SUCCESS# Assuming token is in text format
            elif auth_api_name.lower() == "csp_salesforce":

                object_name = "customer/csp_salesforce/salesforce_token/salesforce_token.txt"

                folder_name="customer/csp_salesforce/salesforce_token"
                file_name="salesforce_token.txt"
                content,status=s3_client.read_s3_file(folder_name,file_name)
                
                print(status)
                print(content)
                if content:
                    token=content
                    print("token --> " + token)
                    if token:
                      self.auth_token = token
                      logger.info("Authentication successful. Token obtained.")
                      return SUCCESS
                else:
                  response, status, status_code = self.make_api_request(auth_method, auth_api, params=params, data=data, headers=headers, timeout=auth_timeout, ssl_verify=auth_ssl_verify)
                  logger.info(f"Response status: {status}")
                  if response and response.status_code == 200:
                      if auth_api_name.lower() == "csp_salesforce":
                          print(response)
                          json_data = response.json()
                          token = json_data.get("access_token")
                          if token:
                            if s3_client.upload_to_s3(token,object_name,kms_key_id,is_gzip=False):
                              logger.info(f"Uploaded csp_salesforce Token to {api_bucket_name}/{object_name}")
                              success = True
                              self.auth_token = token
                              return SUCCESS
                            else:
                              logger.error("csp_salesforce Token upload to S3 failed")
                              success = False
                              return FAILURE
                          else:
                            logger.error("Failed to fetch csp_salesforce token")
                            success = False   
                            return FAILURE                  
                      else:
                          token = response.text.strip()
                          if token:
                            self.auth_token = token
                            logger.info("Authentication successful. Token obtained.")
                            return SUCCESS# Assuming token is in text format

            else:
                response, status, status_code = self.make_api_request(auth_method, auth_api, params=params, data=data, headers=headers, timeout=auth_timeout, ssl_verify=auth_ssl_verify)
                logger.info(f"Response status: {status}")
                if response and response.status_code == 200:
                    if auth_api_name.lower() in ["sharepoint", "contract"]:
                        print(response)
                        json_data = response.json()
                        token = json_data.get("access_token")
                        if token:
                            self.auth_token = token
                            logger.info("Authentication successful. Token obtained.")
                            return SUCCESS
                    else:
                        token = response.text.strip()  # Assuming token is in text format
                        if token:
                            self.auth_token = token
                            logger.info("Authentication successful. Token obtained.")
                            return SUCCESS


            logger.error("Authentication failed. Exiting.")
            return FAILURE
        except Exception as e:
            logger.error(f"Error during authentication: {e}")
            return FAILURE


    def extract_content(self, response, response_type, status_code):
        SUCCESS = True
        FAILURE = False
        status_code = status_code
        try:
            content_extractors = {
                'json': lambda: response.json(),
                'binary': lambda: response.content,
                'xml': lambda: response.content,
                'text': lambda: response.text,
                'raw': lambda: response
            }
            content_extractor = content_extractors.get(response_type, response.json)
            return content_extractor(), SUCCESS, status_code
        except Exception as e:
            print(f"Error extracting content: {e}")
            return None, FAILURE , status_code

    def run(self):
        SUCCESS = True
        FAILURE = False
        try:
            auth_info = self.config.get('auth_api_parameter')
            auth_info = auth_info if auth_info is not None else {}

            auth_api_name = auth_info.get('api_name', 'main')

            if auth_api_name.lower() == "erm_min_ril":
                if self.auth_token is None:
                    if not self.authenticate():
                        return None, FAILURE, None
                else:
                    logger.info("Using existing token for authentication. on erm_min_ril process")
            else:
                if not self.authenticate():
                    return None, FAILURE, None

            api_info = self.config.get('api_parameter')
            job_info = self.config.get('job_parameter')
            api_bucket_name = job_info.get('bucket_name')
            
            
            logger.info(f"API info: {api_info}")
            
            api_endpoint = api_info.get('endpoint')
            api_method = api_info.get('api_method')
            api_params = api_info.get('api_query_params')
            api_data = api_info.get('api_body')
            api_headers = api_info.get('api_headers')
            api_timeout = api_info.get('api_timeout', 600)
            api_ssl_verify = api_info.get('api_ssl_verify', True)
            region = self.config.get('aws_region')
            auth_api_name = auth_info.get('api_name','main') 
            
            s3_client = S3(api_bucket_name, region)

            params = {k: v for k, v in (api_params or {}).items() if v}
            data = api_data if api_data else None
            headers = {k: v for k, v in (api_headers or {}).items() if v}

            if self.auth_token:
                headers['Authorization'] = f'Bearer {self.auth_token}'

            if not all((api_endpoint, api_method)):
                logger.error("Missing required API attributes. Exiting.")
                return None, FAILURE , None
            api_data = json.dumps(data)
            if auth_api_name.lower() == "erm_min_ril" and data:
                api_data = data.encode('utf-8')
                logger.info("erm register in API helper")
            
            response, status, status_code = self.make_api_request(api_method, api_endpoint, params=params, data=api_data, headers=headers, timeout=api_timeout, ssl_verify=api_ssl_verify)
                
            if auth_api_name.lower() == "erm":

              # if status_code is None or status_code == 401:
                if status_code == 401:
                    print("retry response --> " + str(response))
                    print("retry status --> " + str(status))
                    print("retry status_code --> " + str(status_code))
                    print("api_endpoint --> " + api_endpoint)
                    print("api_method --> " + api_method)
                    
                    temp_path_deletion=f"s3://{api_bucket_name}/supply_chain/erm/erm_token/erm_token.txt"    
                    temp_path_deletion = s3_client.get_folder_path_from_s3(temp_path_deletion)
                    logger.info(f"Deleting folder path {temp_path_deletion}")
                    s3_client.delete_folder_from_s3(temp_path_deletion)
                  
                    if not self.authenticate():
                      return None, FAILURE, None
                  
                    if self.auth_token:
                      headers['Authorization'] = f'Bearer {self.auth_token}'
                    api_data = json.dumps(data)
                    if auth_api_name.lower() == "erm_min_ril":
                        api_data = data.encode('utf-8')  
                    response, status, status_code = self.make_api_request(api_method, api_endpoint, params=params, data=api_data, headers=headers, timeout=api_timeout, ssl_verify=api_ssl_verify)              
            elif auth_api_name.lower() == "csp_salesforce":

              # if status_code is None or status_code == 401:
                if status_code == 401:
                    print("retry response --> " + str(response))
                    print("retry status --> " + str(status))
                    print("retry status_code --> " + str(status_code))
                    print("api_endpoint --> " + api_endpoint)
                    print("api_method --> " + api_method)
                    
                    temp_path_deletion=f"s3://{api_bucket_name}/customer/csp_salesforce/salesforce_token/salesforce_token.txt"    
                    temp_path_deletion = s3_client.get_folder_path_from_s3(temp_path_deletion)
                    logger.info(f"Deleting folder path {temp_path_deletion}")
                    s3_client.delete_folder_from_s3(temp_path_deletion)
                  
                    if not self.authenticate():
                      return None, FAILURE, None
                  
                    if self.auth_token:
                      headers['Authorization'] = f'Bearer {self.auth_token}'
                      
                    response, status, status_code = self.make_api_request(api_method, api_endpoint, params=params, data=json.dumps(data), headers=headers, timeout=api_timeout, ssl_verify=api_ssl_verify)              
                
            else:

                if status_code == 401 or status_code == 403 or status_code == 400:
                    logger.error("API request failed. Exiting.")
                    return response, status , status_code

                if response is None:
                    logger.error("API request failed. Exiting.")
                    return None, status , status_code


            content, status, status_code = self.extract_content(response, api_info.get('api_response_type'),status_code)
            if status == FAILURE:
                logger.error("Error extracting content. Exiting.")
                return None, SUCCESS , status_code  #Return SUCCESS as the HTTP API itself returned 200 OK, but an internal logic failed.

            return content, SUCCESS , status_code

        except Exception as e:
            logger.error(f"Error during run: {e}")
            return None, FAILURE, None
