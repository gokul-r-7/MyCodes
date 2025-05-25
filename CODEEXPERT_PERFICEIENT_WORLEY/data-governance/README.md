# Data Governance Repository Activities

## Data Governance v2

### Set up Redshift Idc Application
01. Run the following command in your AWS environment to create Redshift Idc app: 
```bash
aws redshift create-redshift-idc-application --region 'ap-southeast-2' --idc-instance-arn {idc-instance-arn} --identity-namespace 'AWSIDC' --idc-display-name 'worley-datalake-sydney-{env}-redshift-serverless-idc-app' --iam-role-arn 'arn:aws:iam::{account_id}:role/worley-datalake-sydney-{env}-iam-service-role-redshift-awsidc' --redshift-idc-application-name 'worley-datalake-sydney-{env}-redshift-serverless-idc-app'  --service-integrations '[{"LakeFormation":[{"LakeFormationQuery":{"Authorization": "Enabled"}}]}]'
```
note: 
<br>
--idc-instance-arn is the arn of the IAM Identity Center instance

02. Run the following command in your AWS environment to configure trusted identity propagation in QuickSight with Redshift: 
```bash
aws quicksight update-identity-propagation-config --aws-account-id '{account_id}' --service 'REDSHIFT' --authorized-targets '{authorised_targets}'
```
note:
<br>
--authorized-targets is the list of application ARNs that represent the authorized targets
<br>
You can find this in the AWS console by going to "Redshift > IAM Identity Center Connections", clicking on the application, it is the "IAM Identity Center managed application ARN", which starts with "arn:aws:sso".

03. Log onto Redshift Serverless, connect as an admin user, run the following:
```bash
CREATE IDENTITY PROVIDER "worley-datalake-sydney-{env}-redshift-serverless-idc-app" TYPE AWSIDC
NAMESPACE 'AWSIDC' APPLICATION_ARN '{application_arn}'
IAM_ROLE 'arn:aws:iam::{account_id}:role/worley-datalake-sydney-{env}-iam-service-role-redshift-awsidc'
```
note:
<br>
APPLICATION_ARN - You can find this in the AWS console by going to "Redshift > IAM Identity Center Connections", clicking on the application, it is the "IAM Identity Center managed application ARN", which starts with "arn:aws:sso".

```bash
CREATE IDENTITY PROVIDER "worley-datalake-sydney-{env}-redshift-serverless-oauth-app" TYPE azure
NAMESPACE 'AAD'
PARAMETERS '{
"issuer": "https://sts.windows.net/{Microsoft_Azure_tenantid_value}/",
"audience": ["https://analysis.windows.net/powerbi/connector/AmazonRedshift"],
"client_id": "{Microsoft_Azure_clientid_value}",
"client_secret": "{Microsft_Azure_client_secret_value}"
}'
```

To verify these identity providers are created sucessfully, run this command:
```bash
SELECT * from svv_identity_providers
```

### Populate Access Control File in Metadata Repo
In the access_control file, add the transformed database, follow the example - https://github.com/Worley-AWS/metadata/blob/main/dev-templates/aconex/access_control_aconex.yml

### Manange Configuration Files
There are 3 yaml configuration files to manage:
01. lake_permissions.yml - define permissions granted for the lake
02. database_structure.yml - define database specific configurations, including database, external schemas and product schemas. 
03. inside redshift_permissions folder, these are the Redshift permissions files where you define permissions granted to each database. Each database has its individiual file, e.g. "global_standard_reporting.yml" where permissions are defined for the "global_standard_reporting" database only, use this as an example.

There is 1 json settings file to manage, the same file as v1. You will need to add the following configurations in the settings file to support Redshift Serverless:
- "redshift_serverless_host"
- "redshift_serverless_idc_application_arn"
- "redshift_serverless_secret_arn"

### RLS
Put RLS policies for each schema in the "policies/global_standard_reporting/schema" folder. 

As part of v2, we only need to put "rls_create.sql" file. The "rls_create" file is not required, this will be taken care of in the backend. 


## Data Governance v1

01. clone https://github.com/Worley-AWS/data-governance
 
02. Update the governance_config.json https://github.com/Worley-AWS/data-governance/blob/main/configs/dev/governance_config.json to support the new domain. See construction as an example
 
03. You will have to create a secret as its mentioned in the readme.md

04. Create a PR for me or Cindy to Approve
 
metadata repo
<br>

05. Clone https://github.com/Worley-AWS/metadata

06. Create a file call access_control_oracle_p6.yml, follow the example here https://github.com/Worley-AWS/metadata/blob/main/dev-templates/o3/access_control_o3.yml

07. Create a PR for me or Cindy to Approve
 
08. Once both PRs are merged, follow the readme https://github.com/Worley-AWS/data-governance/blob/main/README.md to create a cloud9 instance 

09. And follow the steps to run the lake formation setup and redshift setup 


# How to run this in your own Cloud9 Instance in dev account:
1. Create a Cloud9 instance - with the following configurations:
    <br> vpc: vpc-0ee8f96862c9a9684
    <br> subnet: subnet-06c39f883c76a6e2a
2. Creat a GitHub Personal Access Token, if you don't have one
3. Open your Cloud9 IDE
4. On the left hand side, click on the Git icon (above "AWS" icon)
5. Clone the "data-governance" repository using HTTPS, with your username and PAT
6. In your Cloud9 terminal, run "aws configure sso". When it asks for profile, put "worley-dev-admin"
7. Run "pip install -r requirements.txt" in the terminal 
8. cd into the "data-governance" repository folder, run "python3 xxx", replace "xxx" with the path of the file you want to run, e.g. "infra/src/redshift_setup.py"

## Note:
For granting permissions to dbt users using the scripts, you need to first create the dbt user in the Redshift Query Editor console. This is because you will be creating the user and assigning a password to that user. 
<br>
<br>
The DBA should create a dbt user for each domain, following the naming convention of "dbt_$domain_user", e.g. for "Construction" domain, the username should be "dbt_construction_user".
<br>
<br>
The syntax to create a user in Redshift is:
<br>
create user dbt_$domain_user with password 'insert_your_password';
<br>
<br>
Next, the DBA needs to deploy a secret in secrets manager in the data-platform repository. DBA needs to go to the secrets_manager.tf stack, in line 2 of locals, add in the domain name, e.g. "construction". Link to stack - https://github.com/Worley-AWS/data-platform/blob/main/infra/secrets_manager.tf
<br>
<br>
Once the secret is deployed, the DBA needs to go to the console and update the secret with its credential values:
<br>
<br>
For example,
<br>{
    <br>"username":"dbt_construction_user",
    <br>"password":"your_password_value",
    <br>"engine":"redshift",
    <br>"host":"worley-datawarehouse-sydney-dev-redshift.ciaicozasvrh.ap-southeast-2.redshift.amazonaws.com",
    <br>"port":5439,
    <br>"dbClusterIdentifier":"worley-datawarehouse-sydney-dev-redshift"
<br>}