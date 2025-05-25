import json
import os
from typing import Dict, Any, Tuple
import jwt
import requests

class EntraIDAuthorizer:
    def __init__(self, tenant_id: str, client_id: str):
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.jwks_uri = f"https://login.microsoftonline.com/{tenant_id}/discovery/v2.0/keys"
        self.issuer = f"https://sts.windows.net/{tenant_id}/"

    def get_public_keys(self) -> Dict[str, Any]:
        """Fetch public keys from Microsoft Entra ID"""
        response = requests.get(self.jwks_uri)
        response.raise_for_status()
        return response.json()

    def validate_token(self, token: str) -> Dict[str, Any]:
        """Validate the JWT token and return its payload"""
        keys = self.get_public_keys()
        header = jwt.get_unverified_header(token)
        print(f"Debug - Token Header: {json.dumps(header, indent=2)}")
    
        key_data = next((key for key in keys['keys'] if key['kid'] == header['kid']), None)
        if not key_data:
            raise Exception('Unable to find appropriate key')
    
        public_key = jwt.algorithms.RSAAlgorithm.from_jwk(json.dumps(key_data))
    
        # Decode token without verification for debugging
        # unverified_payload = jwt.decode(token, options={"verify_signature": False})
        # print(f"Debug - Unverified Token Payload: {json.dumps(unverified_payload, indent=2)}")
    
        # Accept both formats of audience
        expected_audiences = [f"api://{self.client_id}", self.client_id]
        print(f"Debug - Expected Audiences: {expected_audiences}")
        print(f"Debug - Expected Issuer: {self.issuer}")
    
        try:
            # Validate token
            payload = jwt.decode(
                token,
                public_key,
                algorithms=['RS256'],
                audience=expected_audiences,  # Use the list of expected audiences, the list might change depending on how Azure sends the response back
                issuer=self.issuer
            )
            print("Debug - Token successfully validated")
            return payload
        except jwt.exceptions.InvalidAudienceError as e:
            print(f"Debug - Audience Validation Failed: {str(e)}")
            raise
        except Exception as e:
            print(f"Debug - Token Validation Failed: {str(e)}")
            raise

    def determine_token_type(self, claims: Dict[str, Any]) -> str:
        """Determine if token is user token or client credentials token"""
        # User tokens typically have these claims
        if 'upn' in claims or 'preferred_username' in claims:
            return 'user'
        # Client credential tokens have app-specific claims
        elif 'azp' in claims or 'appid' in claims:
            return 'client'
        else:
            return 'unknown'

    def get_permissions(self, claims: Dict[str, Any], token_type: str) -> Tuple[str, Dict[str, Any]]:
        """Extract permissions and metadata based on token type"""
        if token_type == 'user':
            # Handle user token
            roles = [role.lower() for role in claims.get('roles', [])]  # Convert roles to lowercase
            metadata = {
                'user_id': claims.get('oid') or claims.get('sub'),
                'email': claims.get('upn', claims.get('preferred_username', '')),
                'name': claims.get('name', ''),
                'token_type': 'user'
            }

            # Simple lowercase comparison
            if 'admin' in roles:
                return 'admin', metadata
            elif 'writer' in roles:
                return 'writer', metadata
            elif 'reader' in roles:
                return 'reader', metadata

        elif token_type == 'client':
            # Handle client credentials token
            scopes = [scope.lower() for scope in claims.get('scp', '').split(' ')]
            roles = [role.lower() for role in claims.get('roles', [])]

            metadata = {
                'client_id': claims.get('appid') or claims.get('azp'),
                'token_type': 'client'
            }

            # Simple lowercase comparison for client credentials
            if 'admin' in roles or 'admin' in scopes:
                return 'admin', metadata
            elif 'writer' in roles or 'write' in scopes:
                return 'writer', metadata
            elif 'reader' in roles or 'read' in scopes:
                return 'reader', metadata
            elif len(roles) <= 0:
                return 'client_app', metadata

        return 'unauthorized', {}


def generate_policy(principal_id: str, effect: str, resource: str, role: str, metadata: Dict[str, Any]) -> Dict[
    str, Any]:
    """Generate IAM policy based on role and include metadata"""
    # Extract the API Gateway ARN parts
    method_arn_parts = resource.split(':')
    region = method_arn_parts[3]
    account_id = method_arn_parts[4]
    api_id = method_arn_parts[5].split('/')[0]
    stage = method_arn_parts[5].split('/')[1]
    # Construct base ARN
    base_arn = f"arn:aws:execute-api:{region}:{account_id}:{api_id}/{stage}"
    policy = {
        'principalId': principal_id,
        'policyDocument': {
            'Version': '2012-10-17',
            'Statement': []
        },
        'context': {
            'role': role,
            'token_type': metadata.get('token_type', 'unknown'),
            **(
                {
                    'userId': metadata.get('user_id', ''),
                    'userEmail': metadata.get('email', ''),
                    'userName': metadata.get('name', '')
                } if metadata.get('token_type') == 'user' else
                {
                    'clientId': metadata.get('client_id', '')
                }
            )
        }
    }
    if role == 'admin':
        policy['policyDocument']['Statement'].append({
            'Action': 'execute-api:Invoke',
            'Effect': effect,
            'Resource': [
                f"{base_arn}/*"  # Allow all methods in the current stage
            ]
        })
    elif role == 'reader':
        policy['policyDocument']['Statement'].append({
            'Action': 'execute-api:Invoke',
            'Effect': effect,
            'Resource': [
                f"{base_arn}/GET/*",  # Allow GET methods in the current stage
                f"{base_arn}/HEAD/*",
                f"{base_arn}/OPTIONS/*"
            ]
        })
    elif role == 'writer':
        policy['policyDocument']['Statement'].append({
            'Action': 'execute-api:Invoke',
            'Effect': effect,
            'Resource': [
                f"{base_arn}/POST/*",
                f"{base_arn}/PUT/*",
                f"{base_arn}/PATCH/*",
                f"{base_arn}/DELETE/*"
            ]
        })
    elif role == 'client_app':
        policy['policyDocument']['Statement'].append({
            'Action': 'execute-api:Invoke',
            'Effect': effect,
            'Resource': [
                f"{base_arn}/*"  # Allow all methods in the current stage
            ]
        })
    print(f"Debug - Generated Policy: {json.dumps(policy, indent=2)}")
    return policy


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Main Lambda handler"""
    try:
        # print("Debug - Event:", json.dumps(event))  # Debug log
        # print(f"Debug - ENTRA_TENANT_ID: {os.environ['ENTRA_TENANT_ID']}")
        # print(f"Debug - ENTRA_CLIENT_ID: {os.environ['ENTRA_CLIENT_ID']}")
        authorizer = EntraIDAuthorizer(
            tenant_id=os.environ['ENTRA_TENANT_ID'],
            client_id=os.environ['ENTRA_CLIENT_ID']
        )
  
        if 'authorizationToken' not in event:
            raise Exception('No authorization token found')
        token = event['authorizationToken'].replace('Bearer ', '')
        
        # Validate token and get claims
        claims = authorizer.validate_token(token)
        
        # Determine token type
        token_type = authorizer.determine_token_type(claims)
        print(f"Debug - Token type: {token_type}")
        
        # Get permissions based on token type
        role, metadata = authorizer.get_permissions(claims, token_type)
        print(f"Debug - Role: {role}")
        print(f"Debug - Metadata: {json.dumps(metadata)}")
        
        if role == 'unauthorized':
            raise Exception('Unauthorized - insufficient permissions')
        
        # Generate policy using the full methodArn
        policy = generate_policy(
            principal_id=metadata.get('user_id', metadata.get('client_id', 'unknown')),
            effect='Allow',
            resource=event['methodArn'],  # Use the full methodArn
            role=role,
            metadata=metadata
        )
        print("Debug - Final Generated Policy:", json.dumps(policy, indent=2))
        return policy
    except Exception as e:
        print(f"Authorization failed: {str(e)}")
        raise Exception('Unauthorized')