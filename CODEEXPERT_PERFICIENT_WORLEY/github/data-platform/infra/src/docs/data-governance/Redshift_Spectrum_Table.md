https://docs.aws.amazon.com/redshift/latest/dg/c-using-spectrum.html
# Redshift Spectrum Table (External Table)
This file documents the steps to create and set up Redshift Spectrum table access, along with its limitations. 

## Create Redshift Spectrum Table (External Table)
Follow these steps to create and set up Redshift Spectrum table access:
1. Sign in to the Redshift console using the admin role
2. Navigate to Query Editor v2
3. Choose the options menu(three dots) next to the cluster and choose <strong> Create connection </strong>
4. Connect as the super admin user and run the following commands to create an external schema from awsdatacatalog and grant access to your Identity Center group:

```
create external schema if not exists <local_schema_name> from DATA CATALOG database <federated_database_name> catalog_id <account_id> 
IAM_ROLE <role_arn>

grant usage on schema <schemaname> to role <idcnamespace:rolename>; 
```

For example:
```
create external schema if not exists test from DATA CATALOG database 'worley_datalake_sydney_dev_glue_catalog_database_aconex' catalog_id '891377181979' 
IAM_ROLE 'arn:aws:iam::891377181979:role/worley-datalake-sydney-dev-iam-service-role-redshift-awsidc'

grant usage on schema test to role "AWSIDC:Test_Data_Consumer"
```

## Applying RLS on external tables
<div style="font-size: 40px;">
⚠️ <strong>Important:</strong>
</div>
Some of the columns in the external tables have data type of "string". When creating a view from these external tables, we need to change the data type of these columns to a data type that is valid in Redshift. For example, we can alter it to be "int" or "varchar".  

If we don't alter the "string" data type, it will error out with "ERROR: incompatible RLS configuration on relation xxxx"

<div style="font-size: 40px;">
⚠️ <strong>Important:</strong>
</div>
We will need to create a new schema to store these views. That is, instead of storing the tables in the source schema, we will create a new schema to store these views.  

If we store the views in the same schema as the source schema, it will error out with "ERROR: Operations on local objects in external schema are not enabled"

This is an example of how you should create a schema to store the views:
```
-- Create a Schema to Store the Views
create schema test_vws_source_aconex

-- Create a View on the TABLE
create view test_vws_source_aconex.vw_curated_project as select 
project::int,
projectname,
projectdescription,
execution_date,
source_system_name,
row_hash,
primary_key,
is_current,
eff_start_date,
eff_end_date
from test_source_aconex.curated_project with no schema binding

-- Grant Permissions
grant usage on schema test_vws_source_aconex to role "AWSIDC:Test_Data_Consumer"
grant select on table test_vws_source_aconex.vw_curated_project to role "AWSIDC:Test_Data_Consumer"

-- Create RLS policy
CREATE RLS POLICY show_project_1207977091_test
WITH (project int)
USING (project = '1207977091');
  

-- Attach Policy
ATTACH RLS POLICY show_project_1207977091_test
ON test_vws_source_aconex.vw_curated_project
TO role "AWSIDC:Test_Data_Consumer"


-- Enable RLS
ALTER table test_vws_source_aconex.vw_curated_project ROW LEVEL security on
```



<div style="font-size: 40px;">
⚠️ <strong>Important:</strong>
</div>

## Limitations
### <ins> Table Level Permissions Not Supported </ins>
Permissions for redshift external tables can only be managed at the schema level; it's not possible to grant permissions for specific tables individually.

For example, if you run the following query:
```
grant usage on schema test to role "AWSIDC:Test_Data_Consumer"

grant select on table test.p6_activity to role "AWSIDC:Test_Data_Consumer"
```

It will return an error message of "operation not supported on external table". This is because external tables can only be managed at the schema level.


# RLS and DDL Support for External Tables in Amazon Redshift

Amazon Redshift does not currently support applying row-level security (RLS) policies or data definition language (DDL) operations directly on external tables.

## Key Points

- You cannot attach RLS policies directly to external tables. RLS policies can only be attached to regular Redshift tables.

- DDL operations like CREATE, ALTER, DROP are supported on external schemas, but not on the individual external tables within those schemas.

- To control access to external tables, you need to grant privileges at the external schema level using GRANT commands with the IAM_ROLE option.

- For finer-grained row-level access control on data in external tables, you would need to use AWS Lake Formation permissions instead of Redshift RLS policies.

## Summary

While RLS and full DDL support is available for regular Redshift tables, these capabilities are limited for external tables which pull data from data lakes or other external sources. The workaround is to use Lake Formation permissions and grant privileges at the external schema level in Redshift.
