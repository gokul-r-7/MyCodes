# Introduction
This document outlines how to onboard a new data engineering team to the data platform, so that they can perform data engineering tasks on the data lake

# Prerequisites 
* AD groups created to meet the ACL policies 
* Tagging policy defined and source tables are tagged accordingly 
* RLS / CLS and DDM based on business needs are identified and policies created in redshift and Lakeformation
* Access to the necessary github repos


# Interfaces 
To perform data engineering tasks successfully, a engineer will have to use the following interfaces 

| Interface | Description|Authentication|
|----|----|:----:|
|Athena (EMR Studio)| To query iceberg tables on the curated data layer | SSO|
|Redshift Query Editor | To query source aligned tables and modelled data | SSO |
|Air flow | Data Orchestrator | SSO|

# Process
1. `Data engineer` to be added the necessary AD groups
2. `Lakeformation Admin` to grant database / RLS / CLS access to the AD groups
3. `RedShift DBA` to grant access to necessary schema and redshift RLS/CLS/DDM policies 
4. AD groups has been added to the relevant IDC application and permission sets mapped to the aws assume'ed role. 

