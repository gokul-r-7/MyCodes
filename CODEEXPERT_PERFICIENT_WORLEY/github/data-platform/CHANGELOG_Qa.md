
#####

## [tag-v0.0.67] - 2024-12-27
### Deployed to:
- **qa**: 2024-12-27
### Changes:
- Deployed version tag-v0.0.67
- Commit Message: Update ecosys_generic_pipeline.py ### Description of changes ###
: added S3 bucket name from airflow

### Files Changed ###
:ecosys_generic_pipeline

### Reason for Change ###
: remove hardcoding

### Task/Ticket/Issue ###
:Ecosys QA deployment - https://dev.azure.com/WorleyDevelopment/AWS%20Transformation/_workitems/edit/52315/
=======
## [tag-v0.0.68] - 2024-12-30
### Deployed to:
- **qa**: 2024-12-30
### Changes:
- Deployed version tag-v0.0.68
- Commit Message: Add files via upload ### Description of changes ###
:Added new pipeline for ecosys onetime load

### Files Changed ###
:ecosys_onetime_load_US_pipeline

### Reason for Change ###
:requirement

### Task/Ticket/Issue ###
:https://dev.azure.com/WorleyDevelopment/AWS%20Transformation/_workitems/edit/52730
- Files Updated: infra/src/data_pipelines/z_dags/ecosys_onetime_load_US_pipeline.py

#####
## [tag-v0.0.69] - 2024-12-30
### Deployed to:
- **qa**: 2024-12-30
### Changes:
- Deployed version tag-v0.0.69
- Commit Message: Update ecosys_onetime_load_US_pipeline.py ### Description of changes ###
:added single task for all tables

### Files Changed ###
:ecosys_onetime_load_US_pipeline

### Reason for Change ###
:change in the sourcing

### Task/Ticket/Issue ###
:https://dev.azure.com/WorleyDevelopment/AWS%20Transformation/_workitems/edit/52730
- Files Updated: infra/src/data_pipelines/z_dags/ecosys_onetime_load_US_pipeline.py

#####
## [tag-v0.0.70] - 2024-12-30
### Deployed to:
- **qa**: 2024-12-30
### Changes:
- Deployed version tag-v0.0.70
- Commit Message: Update ecosys_onetime_load_US_pipeline.py ### Description of changes ###
:correcting the raw tables

### Files Changed ###
:ecosys_onetime_load_US_pipeline

### Reason for Change ###
:correcting the raw tables

### Task/Ticket/Issue ###
:https://dev.azure.com/WorleyDevelopment/AWS%20Transformation/_workitems/edit/52730
- Files Updated: infra/src/data_pipelines/z_dags/ecosys_generic_pipeline.py
infra/src/data_pipelines/z_dags/ecosys_onetime_load_US_pipeline.py

#####
## [tag-v0.0.71] - 2025-01-02
### Deployed to:
- **qa**: 2025-01-02
### Changes:
- Deployed version tag-v0.0.71
- Commit Message: Update ecosys_generic_pipeline.py ### Description of changes ###
:add missing entity for curation

### Files Changed ###
:ecosys_generic_pipeline

### Reason for Change ###
:add missing one

### Task/Ticket/Issue ###
:https://dev.azure.com/WorleyDevelopment/AWS%20Transformation/_workitems/edit/52757/
- Files Updated: infra/src/data_pipelines/z_dags/ecosys_generic_pipeline.py

#####
## [tag-v0.0.72] - 2025-01-03
### Deployed to:
- **qa**: 2025-01-03
### Changes:
- Deployed version tag-v0.0.72
- Commit Message: Update ecosys_generic_pipeline.py ### Description of changes ###
:added missing entity

### Files Changed ###
:ecosys_generic_pipeline

### Reason for Change ###
:added missing entity

### Task/Ticket/Issue ###
:https://dev.azure.com/WorleyDevelopment/AWS%20Transformation/_workitems/edit/52757

- Files Updated: infra/src/data_pipelines/z_dags/ecosys_generic_pipeline.py

#####
## [tag-v0.0.73] - 2025-01-08
### Deployed to:
- **qa**: 2025-01-08
### Changes:
- Deployed version tag-v0.0.73
- Commit Message: Update oracle_gbs_csv_pipeline.py ### Description of changes ###: 2 Currency files promotion to QA ### Files Changed ###: infra/src/data_pipelines/z_dags/oracle_gbs_csv_pipeline.py### Reason for Change ###: Taking 2 currency files to qa and automate the flow for those 2 files to prod asap### Task/Ticket/Issue ###: 51518
- Files Updated: infra/src/data_pipelines/z_dags/oracle_gbs_csv_pipeline.py

#####
## [tag-v0.0.80] - 2025-01-10
### Deployed to:
- **qa**: 2025-01-10
### Changes:
- Deployed version tag-v0.0.80
- Commit Message: Feature/ecosys initialload to2years prod ### Description of changes ###: Ecosys Increased the prod data for 2 years### Files Changed ###: ecosys_generic_sourcing.py, ecosys_generic_pipeline.py### Reason for Change ###: As per the requirement prod needs 2 years data per confirmation from Vikash### Task/Ticket/Issue ###:Ecosys prod changes and deployment - https://dev.azure.com/WorleyDevelopment/AWS%20Transformation/_workitems/edit/52972
- Files Updated: infra/glue_jobs.tf
infra/src/data_pipelines/0_sourcing/ecosys/jobs/ecosys_api_sourcing_job.py
infra/src/data_pipelines/z_dags/ecosys_generic_pipeline.py

#####
## [tag-v0.0.81] - 2025-01-10
### Deployed to:
- **qa**: 2025-01-10
### Changes:
- Deployed version tag-v0.0.81
- Commit Message: Update ecosys_onetime_load_US_pipeline.py ### Description of changes ###:Dag needs to be part of prod deployment### Files Changed ###:ecosys_onetime_load_US_pipeline.py### Reason for Change ###:need for prod deployment### Task/Ticket/Issue ###:Ecosys prod deployment - https://dev.azure.com/WorleyDevelopment/AWS%20Transformation/_workitems/edit/52972
- Files Updated: infra/src/data_pipelines/z_dags/ecosys_onetime_load_US_pipeline.py

#####
## [tag-v0.0.85] - 2025-01-10
### Deployed to:
- **qa**: 2025-01-10
### Changes:
- Deployed version tag-v0.0.85
- Commit Message: Updated CSP scripts ### Description of changes ###:Updated CSP code to include full and delta### Files Changed ###:3 files are updated### Reason for Change ###:Updated CSP code to include full and delta### Task/Ticket/Issue ###:No ticket
- Files Updated: infra/src/data_pipelines/0_sourcing/csp_salesforce/csp_salesforce_job.py
infra/src/data_pipelines/z_dags/csp_salesforce_delta_pipeline.py
infra/src/data_pipelines/z_dags/csp_salesforce_full_pipeline.py

#####
## [tag-v0.0.87] - 2025-01-13
### Deployed to:
- **qa**: 2025-01-13
### Changes:
- Deployed version tag-v0.0.87
- Commit Message: Feature/csp updates ### Description of changes ###:Updated aurora sql job and csp schedulder### Files Changed ###: aurora glue job and csp delta scheduler### Reason for Change ###:Enhancement### Task/Ticket/Issue ###:No ticket
- Files Updated: infra/src/data_pipelines/0_sourcing/aurora/iceberg_aurora_ingestion.py
infra/src/data_pipelines/z_dags/csp_salesforce_delta_pipeline.py

#####
## [tag-v0.0.89] - 2025-01-13
### Deployed to:
- **qa**: 2025-01-13
### Changes:
- Deployed version tag-v0.0.89
- Commit Message: Update oracle_gbs_csv_pipeline.py ### Description of changes ###: GBS currency entities to be enabled in prod### Files Changed ###: oracle_gbs_csv_pipeline.py### Reason for Change ###: enabling schedule### Task/Ticket/Issue ###: ADO task 52945
- Files Updated: infra/src/data_pipelines/z_dags/oracle_gbs_csv_pipeline.py

#####
## [tag-v0.0.92] - 2025-01-15
### Deployed to:
- **qa**: 2025-01-15
### Changes:
- Deployed version tag-v0.0.92
- Commit Message: Update oracle_gbs_csv_pipeline.py ### Description of changes ###: change the schedule of GBS r2 dag### Files Changed ###:oracle_gbs_csv_pipeline.py### Reason for Change ###: change the schedule of GBS r2 dag### Task/Ticket/Issue ###: ADO 52945
- Files Updated: infra/src/data_pipelines/z_dags/oracle_gbs_csv_pipeline.py

#####
## [tag-v0.0.93] - 2025-01-16
### Deployed to:
- **qa**: 2025-01-16
### Changes:
- Deployed version tag-v0.0.93
- Commit Message: Updated csp dag for crawler sensor ### Description of changes ###: Added crawler sensor### Files Changed ###: Daily and Delta csp dag### Reason for Change ###:Enhancement### Task/Ticket/Issue ###:N/A
- Files Updated: infra/src/data_pipelines/z_dags/csp_salesforce_delta_pipeline.py
infra/src/data_pipelines/z_dags/csp_salesforce_full_pipeline.py

#####
## [tag-v0.0.95] - 2025-01-16
### Deployed to:
- **qa**: 2025-01-16
### Changes:
- Deployed version tag-v0.0.95
- Commit Message: update to crawler sensor ### Description of changes ###: Updated crawler sensor### Files Changed ###: Delta and Daily Dag### Reason for Change ###: Crawler sensor### Task/Ticket/Issue ###:NA
- Files Updated: infra/src/data_pipelines/z_dags/csp_salesforce_delta_pipeline.py
infra/src/data_pipelines/z_dags/csp_salesforce_full_pipeline.py

#####
## [tag-v-1.0.4] - 2025-01-17
### Deployed to:
- **qa**: 2025-01-17
### Changes:
- Deployed version tag-v-1.0.4
- Commit Message: no changes just erm dag schedule change ### Description of changes ###: DAG Schedule for ERM to be effective on higher environment### Files Changed ###:erm_sourcing_pipeline.py### Reason for Change ###:from daily once to daily 5.30 AM IST### Task/Ticket/Issue ###:NA
- Files Updated: infra/src/data_pipelines/z_dags/erm_sourcing_pipeline.py

#####
## [tag-v-1.0.8] - 2025-01-20
### Deployed to:
- **qa**: 2025-01-20
### Changes:
- Deployed version tag-v-1.0.8
- Commit Message: Feature/ecosys xml integration ### Description of changes ###: CR:-Integration of xml based sourcing job, create the infra and pipeline### Files Changed ###:glue_jobd.tf### Reason for Change ###:CR - Due to data issue### Task/Ticket/Issue ###:https://dev.azure.com/WorleyDevelopment/AWS%20Transformation/_workitems/edit/52934/
- Files Updated: infra/glue_jobs.tf
infra/src/data_pipelines/0_sourcing/ecosys/jobs/ecosys_api_xml_sourcing_job.py
infra/src/data_pipelines/z_dags/ecosys_xml_generic_pipeline.py

#####
## [tag-v-1.0.12] - 2025-01-20
### Deployed to:
- **qa**: 2025-01-20
### Changes:
- Deployed version tag-v-1.0.12
- Commit Message: Feature/ecosys xml qa deploy ### Description of changes ###: rename the seperate dag### Files Changed ###:ecosys_api_xml_sourcing_job.py### Reason for Change ###:Need seperate dag### Task/Ticket/Issue ###:https://dev.azure.com/WorleyDevelopment/AWS%20Transformation/_workitems/edit/52934/
- Files Updated: infra/src/data_pipelines/0_sourcing/ecosys/jobs/ecosys_api_xml_sourcing_job.py
infra/src/data_pipelines/z_dags/ecosys_xml_generic_pipeline.py

#####
## [tag-v-1.0.14] - 2025-01-21
### Deployed to:
- **qa**: 2025-01-21
### Changes:
- Deployed version tag-v-1.0.14
- Commit Message: Feature/ecosys az ### Description of changes ###: Added new dag for project level pipelinefor HZ and AZ### Files Changed ###:ecosys_project_xml_api_pipeline.py### Reason for Change ###: New requirement fro DC domain### Task/Ticket/Issue ###:https://dev.azure.com/WorleyDevelopment/AWS%20Transformation/_workitems/edit/53200
- Files Updated: infra/src/data_pipelines/0_sourcing/ecosys/jobs/ecosys_api_xml_sourcing_job.py
infra/src/data_pipelines/z_dags/ecosys_project_xml_api_pipeline.py

#####
## [tag-v-1.0.19] - 2025-01-23
### Deployed to:
- **qa**: 2025-01-23
### Changes:
- Deployed version tag-v-1.0.19
- Commit Message: Feature/aconex project update ### Description of changes ###:Added new project### Files Changed ###:DAC files for Aconex UK and KSA### Reason for Change ###:Request for new project### Task/Ticket/Issue ###:N/A
- Files Updated: infra/src/data_pipelines/z_dags/document_control_aconex_ksa1_pipeline.py
infra/src/data_pipelines/z_dags/document_control_aconex_uk_pipeline.py
infra/src/data_pipelines/z_dags/oracle_p6_pipeline_dac_v2.py

#####
## [tag-v-1.0.20] - 2025-01-23
### Deployed to:
- **qa**: 2025-01-23
### Changes:
- Deployed version tag-v-1.0.20
- Commit Message: updated ksa project dag ### Description of changes ###:Updated KSA dag### Files Changed ###:KSA DAG### Reason for Change ###:Added new projects### Task/Ticket/Issue ###:N/A
- Files Updated: infra/src/data_pipelines/z_dags/document_control_aconex_ksa1_pipeline.py

#####
## [tag-v-1.0.21] - 2025-01-23
### Deployed to:
- **qa**: 2025-01-23
### Changes:
- Deployed version tag-v-1.0.21
- Commit Message: Update oracle_gbs_csv_pipeline.py ### Description of changes ###: Adding tasks table### Files Changed ###: infra/src/data_pipelines/z_dags/oracle_gbs_csv_pipeline.py### Reason for Change ###:  Adding tasks table### Task/Ticket/Issue ###: ADO 53203
- Files Updated: infra/src/data_pipelines/z_dags/oracle_gbs_csv_pipeline.py

#####
## [tag-v-1.0.22] - 2025-01-23
### Deployed to:
- **qa**: 2025-01-23
### Changes:
- Deployed version tag-v-1.0.22
- Commit Message: updated KSA project ### Description of changes ###:Updated dag for ksa### Files Changed ###:KSA dag### Reason for Change ###:change req### Task/Ticket/Issue ###:N/A
- Files Updated: infra/src/data_pipelines/z_dags/document_control_aconex_ksa1_pipeline.py

#####
## [tag-v-1.0.28] - 2025-01-24
### Deployed to:
- **qa**: 2025-01-24
### Changes:
- Deployed version tag-v-1.0.28
- Commit Message: Update circuit_breaker_dag.py ### Description of changes ###:Circuit-dag### Files Changed ###:find below### Reason for Change ###:Circuit dag### Task/Ticket/Issue ###:no ticket
- Files Updated: infra/src/data_pipelines/z_dags/circuit_breaker_dag.py

#####
## [tag-v-1.0.29] - 2025-01-24
### Deployed to:
- **qa**: 2025-01-24
### Changes:
- Deployed version tag-v-1.0.29
- Commit Message: Feature/ecosys prod depl ### Description of changes ###:prod deploment### Files Changed ###:ecosys_xml_dag### Reason for Change ###:prod deployment### Task/Ticket/Issue ###:https://dev.azure.com/WorleyDevelopment/AWS%20Transformation/_workitems/edit/52972/
- Files Updated: infra/src/data_pipelines/z_dags/ecosys_project_xml_api_pipeline.py
infra/src/data_pipelines/z_dags/ecosys_xml_generic_pipeline.py

#####
## [tag-v-1.0.30] - 2025-01-27
### Deployed to:
- **qa**: 2025-01-27
### Changes:
- Deployed version tag-v-1.0.30
- Commit Message: Update oracle_gbs_r4_csv_pipeline.py ### Description of changes ###: updating folder details### Files Changed ###: /oracle_gbs_r4_csv_pipeline.py### Reason for Change ###:  updating folder details### Task/Ticket/Issue ###: ADO 51513
- Files Updated: infra/src/data_pipelines/z_dags/oracle_gbs_r4_csv_pipeline.py

#####
## [tag-v-1.0.32] - 2025-01-28
### Deployed to:
- **qa**: 2025-01-28
### Changes:
- Deployed version tag-v-1.0.32
- Commit Message: Feature/gbs dag to prod ### Description of changes ###: Taking GBS dags to prod### Files Changed ###: oracle_gbs_csv_pipeline.py, oracle_gbs_r4_csv_pipeline.py### Reason for Change ###: Taking GBS dags to prod### Task/Ticket/Issue ###: ADO 53202 and 53203
- Files Updated: infra/src/data_pipelines/z_dags/oracle_gbs_csv_pipeline.py
infra/src/data_pipelines/z_dags/oracle_gbs_r4_csv_pipeline.py

#####
## [tag-v-1.0.33] - 2025-01-28
### Deployed to:
- **qa**: 2025-01-28
### Changes:
- Deployed version tag-v-1.0.33
- Commit Message: added logic to stop next run when current run has failed ### Description of changes ###:added logic to stop next run when current run has failed### Files Changed ###:added logic to stop next run when current run has failed### Reason for Change ###:added logic to stop next run when current run has failed### Task/Ticket/Issue ###:added logic to stop next run when current run has failed
- Files Updated: infra/src/data_pipelines/z_dags/erm_sourcing_pipeline.py

#####
## [tag-v-1.0.35] - 2025-01-28
### Deployed to:
- **qa**: 2025-01-28
### Changes:
- Deployed version tag-v-1.0.35
- Commit Message: Feature/genai notification fix ### Description of changes ###:GenAI schedule change### Files Changed ###:GenAI ped dag### Reason for Change ###:Enhancement### Task/Ticket/Issue ###:N/A
- Files Updated: infra/src/data_pipelines/0_sourcing/genai/ped_dataverse_job.py
infra/src/data_pipelines/0_sourcing/genai/sharepoint_job.py
infra/src/data_pipelines/z_dags/genai_ped_generic_pipeline.py

#####
## [tag-v-1.0.34] - 2025-01-29
### Deployed to:
- **qa**: 2025-01-29
### Changes:
- Deployed version tag-v-1.0.34
- Commit Message: updates to aconex jobs ### Description of changes ###:Changes to Aconex dag and sourcing jobs to handle instance level curation### Files Changed ###:Aconex sourcing job,DAG### Reason for Change ###:Enhancement### Task/Ticket/Issue ###:N/A
- Files Updated: infra/glue_jobs.tf
infra/src/data_pipelines/0_sourcing/aconex/jobs/aconex_UserDirectory_api_sourcing_job.py
infra/src/data_pipelines/0_sourcing/aconex/jobs/aconex_UserProjectRole_api_sourcing_job.py
infra/src/data_pipelines/0_sourcing/aconex/jobs/aconex_UserProject_api_sourcing_job.py
infra/src/data_pipelines/0_sourcing/aconex/jobs/aconex_project_api_sourcing_job.py
infra/src/data_pipelines/z_dags/aconex_aus_pipeline.py
infra/src/data_pipelines/z_dags/aconex_ca_pipeline.py
infra/src/data_pipelines/z_dags/aconex_cn_pipeline.py
infra/src/data_pipelines/z_dags/aconex_hk_pipeline.py
infra/src/data_pipelines/z_dags/aconex_ksa_pipeline.py
infra/src/data_pipelines/z_dags/aconex_mea_pipeline.py
infra/src/data_pipelines/z_dags/aconex_uk_pipeline.py
infra/src/data_pipelines/z_dags/aconex_us_pipeline.py
infra/src/data_pipelines/z_dags/document_control_aconex_ksa1_pipeline.py

#####
## [tag-v-1.0.36] - 2025-01-29
### Deployed to:
- **qa**: 2025-01-29
### Changes:
- Deployed version tag-v-1.0.36
- Commit Message: Feature/aconex pipeline instance ### Description of changes ###: missing the dag userproject roles### Files Changed ###: infra/src/data_pipelines/z_dags/aconex_aus_pipeline.py### Reason for Change ###: Missing the entity### Task/Ticket/Issue ###:https://dev.azure.com/WorleyDevelopment/AWS%20Transformation/_workitems/edit/53463/
- Files Updated: infra/src/data_pipelines/0_sourcing/aconex/jobs/aconex_UserDirectory_api_sourcing_job.py
infra/src/data_pipelines/0_sourcing/aconex/jobs/aconex_UserProjectRole_api_sourcing_job.py
infra/src/data_pipelines/0_sourcing/aconex/jobs/aconex_UserProject_api_sourcing_job.py
infra/src/data_pipelines/0_sourcing/aconex/jobs/aconex_mail_api_sourcing_job.py
infra/src/data_pipelines/0_sourcing/aconex/jobs/aconex_mail_document_api_sourcing_job.py
infra/src/data_pipelines/z_dags/aconex_aus_pipeline.py
infra/src/data_pipelines/z_dags/aconex_ca_pipeline.py
infra/src/data_pipelines/z_dags/aconex_cn_pipeline.py
infra/src/data_pipelines/z_dags/aconex_hk_pipeline.py
infra/src/data_pipelines/z_dags/aconex_ksa_pipeline.py
infra/src/data_pipelines/z_dags/aconex_mea_pipeline.py
infra/src/data_pipelines/z_dags/aconex_uk_pipeline.py
infra/src/data_pipelines/z_dags/aconex_us_pipeline.py

#####
## [tag-v-1.0.39] - 2025-01-30
### Deployed to:
- **qa**: 2025-01-30
### Changes:
- Deployed version tag-v-1.0.39
- Commit Message: Feature/dbt connec source ### Description of changes ###: include sns and dbt dag event trigger### Files Changed ###:aconex_xxx_pipeline.py### Reason for Change ###: failure notification and handshake intimation for flow### Task/Ticket/Issue ###:https://dev.azure.com/WorleyDevelopment/AWS%20Transformation/_workitems/edit/53463/
- Files Updated: infra/src/data_pipelines/z_dags/aconex_aus_pipeline.py
infra/src/data_pipelines/z_dags/aconex_ca_pipeline.py
infra/src/data_pipelines/z_dags/aconex_cn_pipeline.py
infra/src/data_pipelines/z_dags/aconex_hk_pipeline.py
infra/src/data_pipelines/z_dags/aconex_ksa_pipeline.py
infra/src/data_pipelines/z_dags/aconex_mea_pipeline.py
infra/src/data_pipelines/z_dags/aconex_uk_pipeline.py
infra/src/data_pipelines/z_dags/aconex_us_pipeline.py

#####
## [tag-v-1.0.41] - 2025-01-30
### Deployed to:
- **qa**: 2025-01-30
### Changes:
- Deployed version tag-v-1.0.41
- Commit Message: Update circuit_breaker_dag.py ### Description of changes ###:circuit_breaker_dag.py### Files Changed ###:circuit_breaker_dag.py### Reason for Change ###:circuit_breaker_dag.py### Task/Ticket/Issue ###:circuit_breaker_dag.py
- Files Updated: infra/src/data_pipelines/z_dags/circuit_breaker_dag.py

#####
## [tag-v-1.0.42] - 2025-02-03
### Deployed to:
- **qa**: 2025-02-03
### Changes:
- Deployed version tag-v-1.0.42
- Commit Message: Feature/gbs r4 schedule change ### Description of changes ###: Changes to r4 dag schedule and sftp csv file### Files Changed ###: sftp_sourcing_job.py,  oracle_gbs_r4_csv_pipeline.py### Reason for Change ###: after confirming with support, changing r4 schedule to 1 PM### Task/Ticket/Issue ###: ADO 53202
- Files Updated: infra/src/data_pipelines/0_sourcing/sftp/sftp_sourcing_job.py
infra/src/data_pipelines/z_dags/oracle_gbs_r4_csv_pipeline.py

#####
## [tag-v-1.0.52] - 2025-02-05
### Deployed to:
- **qa**: 2025-02-05
### Changes:
- Deployed version tag-v-1.0.52
- Commit Message: Updated ecodata schedule ### Description of changes ###:Updated DAG schedule for Ecosys### Files Changed ###:Ecosys DAG### Reason for Change ###:Requirement### Task/Ticket/Issue ###:N/A
- Files Updated: infra/src/data_pipelines/z_dags/ecosys_xml_generic_pipeline.py

#####
## [tag-v-1.0.57] - 2025-02-06
### Deployed to:
- **qa**: 2025-02-06
### Changes:
- Deployed version tag-v-1.0.57
- Commit Message: Added new DAC to handle O3 initial load ### Description of changes ###:Added new dag for O3 DAC### Files Changed ###:New schedule### Reason for Change ###:New load### Task/Ticket/Issue ###:N/A
- Files Updated: infra/src/data_pipelines/z_dags/o3_pipeline_dac.py

#####
## [tag-v-1.0.62] - 2025-02-07
### Deployed to:
- **qa**: 2025-02-07
### Changes:
- Deployed version tag-v-1.0.62
- Commit Message: Feature/aconex userrole schema ### Description of changes ###: Removed the userrole and updated the schedules for each region at midnight### Files Changed ###: aconex_us_pipeline.py### Reason for Change ###: change request### Task/Ticket/Issue ###: Adhoc task
- Files Updated: infra/src/data_pipelines/z_dags/aconex_ksa_pipeline.py
infra/src/data_pipelines/z_dags/aconex_uk_pipeline.py
infra/src/data_pipelines/z_dags/aconex_us_pipeline.py

#####
## [tag-v-1.0.64] - 2025-02-10
### Deployed to:
- **qa**: 2025-02-10
### Changes:
- Deployed version tag-v-1.0.64
- Commit Message: Feature/acnx dag ### Description of changes ###:new changes of schedule### Files Changed ###:aconex_us_pipeline.py, aconex_uk_pipeline.py### Reason for Change ###: schedule change### Task/Ticket/Issue ###:https://dev.azure.com/WorleyDevelopment/AWS%20Transformation/_workitems/edit/52936
- Files Updated: infra/src/data_pipelines/z_dags/aconex_ksa_pipeline.py
infra/src/data_pipelines/z_dags/aconex_uk_pipeline.py
infra/src/data_pipelines/z_dags/aconex_us_pipeline.py
infra/src/data_pipelines/z_dags/ecosys_xml_generic_pipeline.py

#####
## [tag-v-1.0.72] - 2025-02-12
### Deployed to:
- **qa**: 2025-02-12
### Changes:
- Deployed version tag-v-1.0.72
- Commit Message: Update hexagon_US_pipeline.py ### Description of changes ###: added sns notification### Files Changed ###: hexagon_US_pipeline.py### Reason for Change ###: Need to add sns notification### Task/Ticket/Issue ###:https://dev.azure.com/WorleyDevelopment/AWS%20Transformation/_workitems/edit/53764
- Files Updated: infra/src/data_pipelines/z_dags/hexagon_US_pipeline.py

#####
## [tag-v-1.0.73] - 2025-02-12
### Deployed to:
- **qa**: 2025-02-12
### Changes:
- Deployed version tag-v-1.0.73
- Commit Message: Update hexagon_US_pipeline.py ### Description of changes ###: added more control### Files Changed ###:hexagon_US_pipeline.py### Reason for Change ###: enhancement### Task/Ticket/Issue ###:https://dev.azure.com/WorleyDevelopment/AWS%20Transformation/_workitems/edit/53764
- Files Updated: infra/src/data_pipelines/z_dags/hexagon_US_pipeline.py

#####
## [tag-v-1.0.75] - 2025-02-12
### Deployed to:
- **qa**: 2025-02-12
### Changes:
- Deployed version tag-v-1.0.75
- Commit Message: Update hexagon_US_pipeline.py ### Description of changes ###: correcting the crawler sensor### Files Changed ###:hexagon_US_pipeline.py### Reason for Change ###:correcting the crawler### Task/Ticket/Issue ###:https://dev.azure.com/WorleyDevelopment/AWS%20Transformation/_workitems/edit/53764
- Files Updated: infra/src/data_pipelines/z_dags/hexagon_US_pipeline.py

#####
## [tag-v-1.0.76] - 2025-02-12
### Deployed to:
- **qa**: 2025-02-12
### Changes:
- Deployed version tag-v-1.0.76
- Commit Message: updated delta schedule ### Description of changes ###:Update to CSP delta schedule### Files Changed ###:CSP Delta schedule### Reason for Change ###:To avoid conflict with full load### Task/Ticket/Issue ###:N/A
- Files Updated: infra/src/data_pipelines/z_dags/csp_salesforce_delta_pipeline.py

#####
## [tag-v-1.0.77] - 2025-02-12
### Deployed to:
- **qa**: 2025-02-12
### Changes:
- Deployed version tag-v-1.0.77
- Commit Message: scheduling dag's in qa ### Description of changes ###: QA deployment of engg_dags and smb connector### Files Changed ###: engg_dag's and smb connector files### Reason for Change ###: QA deployment of engg_dags and smb connector### Task/Ticket/Issue ###: qa deployment ado 53760
- Files Updated: infra/glue_jobs.tf
infra/src/data_pipelines/z_dags/aim_pipeline.py
infra/src/data_pipelines/z_dags/e3d_ado_pipeline.py
infra/src/data_pipelines/z_dags/e3d_main_pipeline.py
infra/src/data_pipelines/z_dags/e3d_mto_pipeline.py
infra/src/data_pipelines/z_dags/engreg_sharepointlist_pipeline.py
infra/src/data_pipelines/z_dags/isotracker_pipeline.py
infra/src/data_pipelines/z_dags/mel_pipeline.py
infra/src/data_pipelines/z_dags/mem_pipeline.py
infra/src/data_pipelines/z_dags/mps_pipeline.py
infra/src/data_pipelines/z_dags/omie_pipeline.py
infra/src/data_pipelines/z_dags/pdm_pipeline.py
infra/src/data_pipelines/z_dags/spel_pipeline.py
infra/src/data_pipelines/z_dags/spi_pipeline.py
infra/src/data_pipelines/z_dags/spid_pipeline.py
infra/src/smb_connector/smb_connector.py
infra/src/worley_helper/worley_helper/configuration/config.py
infra/src/worley_helper/worley_helper/configuration/ingest.py
infra/src/worley_helper/worley_helper/utils/smb.py

#####
## [tag-v-1.0.84] - 2025-02-14
### Deployed to:
- **qa**: 2025-02-14
### Changes:
- Deployed version tag-v-1.0.84
- Commit Message: made changes in dags ### Description of changes ###: Made changes in DAGs### Files Changed ###: aim, mem, omie, spel, pdm, spi, spid### Reason for Change ###: Made changes in DAGs ### Task/Ticket/Issue ###: No Ticket
- Files Updated: infra/src/data_pipelines/z_dags/aim_pipeline.py
infra/src/data_pipelines/z_dags/mem_pipeline.py
infra/src/data_pipelines/z_dags/omie_pipeline.py
infra/src/data_pipelines/z_dags/pdm_pipeline.py
infra/src/data_pipelines/z_dags/spel_pipeline.py
infra/src/data_pipelines/z_dags/spi_pipeline.py
infra/src/data_pipelines/z_dags/spid_pipeline.py

#####
## [tag-v-1.0.85] - 2025-02-14
### Deployed to:
- **qa**: 2025-02-14
### Changes:
- Deployed version tag-v-1.0.85
- Commit Message: Updated DAG for delta ### Description of changes ###:DAG Update for CSP### Files Changed ###:Update to schedule### Reason for Change ###:To avoid clash of Full and Delta### Task/Ticket/Issue ###:N/A
- Files Updated: infra/src/data_pipelines/z_dags/csp_salesforce_delta_pipeline.py

#####
## [tag-v-1.0.86] - 2025-02-14
### Deployed to:
- **qa**: 2025-02-14
### Changes:
- Deployed version tag-v-1.0.86
- Commit Message: made changes in DAGs ### Description of changes ###: made changes in DAGs### Files Changed ###: aim, mem, omie, spel, pdm, spi, spid### Reason for Change ###: made changes in DAGs### Task/Ticket/Issue ###: No ticket
- Files Updated: infra/src/data_pipelines/z_dags/aim_pipeline.py
infra/src/data_pipelines/z_dags/mem_pipeline.py
infra/src/data_pipelines/z_dags/omie_pipeline.py
infra/src/data_pipelines/z_dags/pdm_pipeline.py
infra/src/data_pipelines/z_dags/spel_pipeline.py
infra/src/data_pipelines/z_dags/spi_pipeline.py
infra/src/data_pipelines/z_dags/spid_pipeline.py

#####
## [tag-v-1.0.92] - 2025-02-14
### Deployed to:
- **qa**: 2025-02-14
### Changes:
- Deployed version tag-v-1.0.92
- Commit Message: Segregated the users entities into a seperate dag ### Description of changes ###: Created the users entities dedicated pipeline### Files Changed ###: aconex_xx_pipeline.py### Reason for Change ###: a change request to create a new dags### Task/Ticket/Issue ###:https://dev.azure.com/WorleyDevelopment/AWS%20Transformation/_workitems/edit/53794/
- Files Updated: infra/src/data_pipelines/z_dags/aconex_aus_pipeline.py
infra/src/data_pipelines/z_dags/aconex_aus_users_pipeline.py
infra/src/data_pipelines/z_dags/aconex_ca_pipeline.py
infra/src/data_pipelines/z_dags/aconex_ca_users_pipeline.py
infra/src/data_pipelines/z_dags/aconex_cn_pipeline.py
infra/src/data_pipelines/z_dags/aconex_cn_users_pipeline.py
infra/src/data_pipelines/z_dags/aconex_hk_pipeline.py
infra/src/data_pipelines/z_dags/aconex_hk_users_pipeline.py
infra/src/data_pipelines/z_dags/aconex_ksa_pipeline.py
infra/src/data_pipelines/z_dags/aconex_ksa_users_pipeline.py
infra/src/data_pipelines/z_dags/aconex_mea_pipeline.py
infra/src/data_pipelines/z_dags/aconex_mea_users_pipeline.py
infra/src/data_pipelines/z_dags/aconex_uk_pipeline.py
infra/src/data_pipelines/z_dags/aconex_uk_users_pipeline.py
infra/src/data_pipelines/z_dags/aconex_us_pipeline.py
infra/src/data_pipelines/z_dags/aconex_us_users_pipeline.py

#####
## [tag-v-1.0.99] - 2025-02-17
### Deployed to:
- **qa**: 2025-02-17
### Changes:
- Deployed version tag-v-1.0.99
- Commit Message: fixing crawler issue ### Description of changes ###: Changing dag crawler issue### Files Changed ###:  All engg file system dags### Reason for Change ###: Changing dag crawler issue### Task/Ticket/Issue ###: automation
- Files Updated: infra/src/data_pipelines/z_dags/e3d_ado_pipeline.py
infra/src/data_pipelines/z_dags/e3d_main_pipeline.py
infra/src/data_pipelines/z_dags/e3d_mto_pipeline.py
infra/src/data_pipelines/z_dags/engreg_sharepointlist_pipeline.py
infra/src/data_pipelines/z_dags/isotracker_pipeline.py
infra/src/data_pipelines/z_dags/mel_pipeline.py
infra/src/data_pipelines/z_dags/mps_pipeline.py

#####
## [tag-v-1.0.98] - 2025-02-17
### Deployed to:
- **qa**: 2025-02-17
### Changes:
- Deployed version tag-v-1.0.98
- Commit Message: Feature/aconex doc ### Description of changes ###: added instance### Files Changed ###: document_control_us_pipeline### Reason for Change ###: new change request### Task/Ticket/Issue ###:https://dev.azure.com/WorleyDevelopment/AWS%20Transformation/_workitems/edit/53794
- Files Updated: infra/src/data_pipelines/z_dags/document_control_aconex_uk_pipeline.py
infra/src/data_pipelines/z_dags/document_control_aconex_us_pipeline.py

#####
## [tag-v-1.0.100] - 2025-02-17
### Deployed to:
- **qa**: 2025-02-17
### Changes:
- Deployed version tag-v-1.0.100
- Commit Message: Update engreg_sharepointlist_pipeline.py ### Description of changes ###:  engreg_sharepointlist_pipeline dag glue job changes### Task/Ticket/Issue ###: engreg automation in qa
- Files Updated: infra/src/data_pipelines/z_dags/engreg_sharepointlist_pipeline.py

#####
## [tag-v-1.0.102] - 2025-02-18
### Deployed to:
- **qa**: 2025-02-18
### Changes:
- Deployed version tag-v-1.0.102
- Commit Message: QA Deployment ### Description of changes ###:Added Minor Chnages for QA Deployment### Task/Ticket/Issue ###:QA
- Files Updated: infra/glue_jobs.tf
infra/src/data_pipelines/0_sourcing/csv/jobs/convert_aconex_csv_and_xlsx_to_parquet.py
infra/src/data_pipelines/z_dags/aconex_bot_euazrenc001v_pipeline.py
infra/src/data_pipelines/z_dags/aconex_bot_sgsdcwpenc02v_pipeline.py
infra/src/data_pipelines/z_dags/aconex_cacdcwpspf06v_pipeline.py
infra/src/smb_connector/smb_connector_bot_files.py
infra/src/worley_helper/worley_helper/utils/smb.py

#####
## [tag-v-1.0.105] - 2025-02-19
### Deployed to:
- **qa**: 2025-02-19
### Changes:
- Deployed version tag-v-1.0.105
- Commit Message: Updated CSP schedule ### Description of changes ###:CSP Dag update### Task/Ticket/Issue ###:N/A
- Files Updated: infra/src/data_pipelines/z_dags/csp_salesforce_delta_pipeline.py

#####
## [tag-v-1.0.108] - 2025-02-21
### Deployed to:
- **qa**: 2025-02-21
### Changes:
- Deployed version tag-v-1.0.108
- Commit Message: Update ecosys_xml_generic_pipeline.py ### Description of changes ###:### Task/Ticket/Issue ###:
- Files Updated: infra/src/data_pipelines/z_dags/ecosys_xml_generic_pipeline.py

#####
## [tag-v-1.0.115] - 2025-02-25
### Deployed to:
- **qa**: 2025-02-25
### Changes:
- Deployed version tag-v-1.0.115
- Commit Message: Update to the csp pipeline ### Description of changes ###:Updated csp pipeline### Task/Ticket/Issue ###:N/A
- Files Updated: infra/src/data_pipelines/z_dags/csp_salesforce_delta_pipeline.py
infra/src/data_pipelines/z_dags/csp_salesforce_full_pipeline.py

#####
## [tag-v-1.0.116] - 2025-02-25
### Deployed to:
- **qa**: 2025-02-25
### Changes:
- Deployed version tag-v-1.0.116
- Commit Message: updated csp pipeline ### Description of changes ###:Updates CSP pipeline### Task/Ticket/Issue ###:N/A
- Files Updated: infra/src/data_pipelines/z_dags/csp_salesforce_curation_pipeline.py
infra/src/data_pipelines/z_dags/csp_salesforce_delta_pipeline.py
infra/src/data_pipelines/z_dags/csp_salesforce_full_pipeline.py

#####
## [tag-v-1.0.112] - 2025-02-25
### Deployed to:
- **qa**: 2025-02-25
### Changes:
- Deployed version tag-v-1.0.112
- Commit Message: Feature/r2 dag fix ### Description of changes ###: curation job upgraded version and r2 dag fix### Task/Ticket/Issue ###: GBS incremental load fix
- Files Updated: infra/glue_jobs.tf
infra/src/data_pipelines/z_dags/oracle_gbs_csv_pipeline.py

#####
## [tag-v-1.0.119] - 2025-02-26
### Deployed to:
- **qa**: 2025-02-26
### Changes:
- Deployed version tag-v-1.0.119
- Commit Message: added retry and updated schedule ### Description of changes ###:added retry and updated schedule### Task/Ticket/Issue ###:added retry and updated schedule
- Files Updated: infra/src/data_pipelines/z_dags/erm_sourcing_pipeline.py

#####
## [tag-v-1.0.120] - 2025-02-26
### Deployed to:
- **qa**: 2025-02-26
### Changes:
- Deployed version tag-v-1.0.120
- Commit Message: Update document_control_aconex_uk_pipeline.py ### Description of changes ###: added new projects as new cr### Task/Ticket/Issue ###:https://dev.azure.com/WorleyDevelopment/AWS%20Transformation/_workitems/edit/54178/
- Files Updated: infra/src/data_pipelines/z_dags/document_control_aconex_uk_pipeline.py

#####
## [tag-v-1.0.121] - 2025-02-27
### Deployed to:
- **qa**: 2025-02-27
### Changes:
- Deployed version tag-v-1.0.121
- Commit Message: Update document_control_aconex_us_pipeline.py ### Description of changes ###: changes for the instance or entity load implementation### Task/Ticket/Issue ###:https://dev.azure.com/WorleyDevelopment/AWS%20Transformation/_workitems/edit/54189
- Files Updated: infra/src/data_pipelines/z_dags/document_control_aconex_us_pipeline.py

#####
## [tag-v-1.0.122] - 2025-02-28
### Deployed to:
- **qa**: 2025-02-28
### Changes:
- Deployed version tag-v-1.0.122
- Commit Message: Feature/dag changes ### Description of changes ###: dag changes in isotracker and mel### Task/Ticket/Issue ###: DAG changes for prod deployment
- Files Updated: infra/src/data_pipelines/z_dags/isotracker_pipeline.py
infra/src/data_pipelines/z_dags/mel_pipeline.py

#####
## [tag-v-1.0.123] - 2025-03-03
### Deployed to:
- **qa**: 2025-03-03
### Changes:
- Deployed version tag-v-1.0.123
- Commit Message: Feature/dag deployment engg ### Description of changes ###: Prod dag deployment of engineering stream### Task/Ticket/Issue ###: Prod deployment
- Files Updated: infra/src/data_pipelines/z_dags/aim_pipeline.py
infra/src/data_pipelines/z_dags/e3d_ado_pipeline.py
infra/src/data_pipelines/z_dags/e3d_main_pipeline.py
infra/src/data_pipelines/z_dags/e3d_mto_pipeline.py
infra/src/data_pipelines/z_dags/engreg_sharepointlist_pipeline.py
infra/src/data_pipelines/z_dags/isotracker_pipeline.py
infra/src/data_pipelines/z_dags/mel_pipeline.py
infra/src/data_pipelines/z_dags/mem_pipeline.py
infra/src/data_pipelines/z_dags/mps_pipeline.py
infra/src/data_pipelines/z_dags/omie_pipeline.py
infra/src/data_pipelines/z_dags/pdm_pipeline.py
infra/src/data_pipelines/z_dags/spel_pipeline.py
infra/src/data_pipelines/z_dags/spi_pipeline.py
infra/src/data_pipelines/z_dags/spid_pipeline.py

#####
## [tag-v-1.0.124] - 2025-03-03
### Deployed to:
- **qa**: 2025-03-03
### Changes:
- Deployed version tag-v-1.0.124
- Commit Message: Feature/aconex dags ### Description of changes ###: QA deployment### Task/Ticket/Issue ###: NA
- Files Updated: infra/src/data_pipelines/z_dags/document_control_aconex_uk_pipeline.py
infra/src/data_pipelines/z_dags/document_control_aconex_us_pipeline.py

#####
## [tag-v-1.0.125] - 2025-03-03
### Deployed to:
- **qa**: 2025-03-03
### Changes:
- Deployed version tag-v-1.0.125
- Commit Message: Update document_control_aconex_uk_pipeline.py ### Description of changes ###:dag changes### Task/Ticket/Issue ###:NA
- Files Updated: infra/src/data_pipelines/z_dags/document_control_aconex_uk_pipeline.py

#####
## [tag-v-1.0.126] - 2025-03-04
### Deployed to:
- **qa**: 2025-03-04
### Changes:
- Deployed version tag-v-1.0.126
- Commit Message: adding catchup=False property ### Description of changes ###: adding catchup=False property### Task/Ticket/Issue ###: PROD DAG deployment
- Files Updated: infra/src/data_pipelines/z_dags/aim_pipeline.py
infra/src/data_pipelines/z_dags/e3d_ado_pipeline.py
infra/src/data_pipelines/z_dags/e3d_main_pipeline.py
infra/src/data_pipelines/z_dags/e3d_mto_pipeline.py
infra/src/data_pipelines/z_dags/engreg_sharepointlist_pipeline.py
infra/src/data_pipelines/z_dags/isotracker_pipeline.py
infra/src/data_pipelines/z_dags/mel_pipeline.py
infra/src/data_pipelines/z_dags/mem_pipeline.py
infra/src/data_pipelines/z_dags/mps_pipeline.py
infra/src/data_pipelines/z_dags/omie_pipeline.py
infra/src/data_pipelines/z_dags/pdm_pipeline.py
infra/src/data_pipelines/z_dags/spel_pipeline.py
infra/src/data_pipelines/z_dags/spi_pipeline.py
infra/src/data_pipelines/z_dags/spid_pipeline.py

#####
## [tag-v-1.0.118] - 2025-03-04
### Deployed to:
- **qa**: 2025-03-04
### Changes:
- Deployed version tag-v-1.0.118
- Commit Message: Update ecosys_xml_generic_pipeline.py ### Description of changes ###:projectKeyMembers pipeline ### Task/Ticket/Issue ###:54123
- Files Updated: infra/src/data_pipelines/z_dags/ecosys_xml_generic_pipeline.py

#####
## [tag-v-1.0.132] - 2025-03-06
### Deployed to:
- **qa**: 2025-03-06
### Changes:
- Deployed version tag-v-1.0.132
- Commit Message: Feature/scm transformed ### Description of changes ###:added dag dependency### Task/Ticket/Issue ###:added dag dependency
- Files Updated: infra/src/data_pipelines/z_dags/supply_chain_ril_min.py

#####
## [tag-v-1.0.135] - 2025-03-06
### Deployed to:
- **qa**: 2025-03-06
### Changes:
- Deployed version tag-v-1.0.135
- Commit Message: fixing dag scheduler for sharepoint_gbs_pipeline ### Description of changes ###: Fix the broken dag because of scheduler### Task/Ticket/Issue ###: 53789
- Files Updated: infra/src/data_pipelines/z_dags/sharepoint_gbs_pipeline.py

#####
## [tag-v-1.0.137] - 2025-03-07
### Deployed to:
- **qa**: 2025-03-07
### Changes:
- Deployed version tag-v-1.0.137
- Commit Message: marked the wait_for_completion flag to False ### Description of changes ###: Marked the crawler wait_for_completion flag to False as IAM policy is not live yet### Task/Ticket/Issue ###: 53789
- Files Updated: infra/src/data_pipelines/z_dags/sharepoint_gbs_pipeline.py

#####
## [tag-v-1.0.140] - 2025-03-12
### Deployed to:
- **qa**: 2025-03-12
### Changes:
- Deployed version tag-v-1.0.140
- Commit Message: Feature/erm register ### Description of changes ###: erm -changes### Task/Ticket/Issue ###:https://dev.azure.com/WorleyDevelopment/AWS%20Transformation/_workitems/edit/54265/
- Files Updated: infra/glue_jobs.tf
infra/src/data_pipelines/0_sourcing/erm/jobs/erm_min_api_sourcing_job.py
infra/src/data_pipelines/z_dags/document_control_aconex_uk_pipeline.py
infra/src/worley_helper/worley_helper/utils/http_api_client.py

#####
## [tag-v-1.0.142] - 2025-03-13
### Deployed to:
- **qa**: 2025-03-13
### Changes:
- Deployed version tag-v-1.0.142
- Commit Message: updated package dac ### Description of changes ###:Dag updates for packages### Task/Ticket/Issue ###:N/A
- Files Updated: infra/src/data_pipelines/z_dags/aconex_package_cacdcwpspf06v_pipeline.py

#####
## [tag-v-1.0.143] - 2025-03-18
### Deployed to:
- **qa**: 2025-03-18
### Changes:
- Deployed version tag-v-1.0.143
- Commit Message: Update supply_chain_ril_min.py ### Description of changes ###: dag changes### Task/Ticket/Issue ###:NA
- Files Updated: infra/src/data_pipelines/z_dags/supply_chain_ril_min.py

#####
## [tag-v-1.0.144] - 2025-03-18
### Deployed to:
- **qa**: 2025-03-18
### Changes:
- Deployed version tag-v-1.0.144
- Commit Message: promote the DAG to QA ### Description of changes ###: promoting the DAG to QA### Task/Ticket/Issue ###:57389
- Files Updated: infra/src/data_pipelines/z_dags/supply_chain_ril_min.py

#####
## [tag-v-1.0.147] - 2025-03-25
### Deployed to:
- **qa**: 2025-03-25
### Changes:
- Deployed version tag-v-1.0.147
- Commit Message: updated schedule for PED and CSP to aest ### Description of changes ###:Update to PED and CSP schedule### Task/Ticket/Issue ###:N/A
- Files Updated: infra/src/data_pipelines/z_dags/csp_salesforce_delta_pipeline.py
infra/src/data_pipelines/z_dags/csp_salesforce_full_pipeline.py
infra/src/data_pipelines/z_dags/genai_ped_generic_pipeline.py

#####
## [tag-v-1.0.149] - 2025-03-28
### Deployed to:
- **qa**: 2025-03-28
### Changes:
- Deployed version tag-v-1.0.149
- Commit Message: Feature/pc v2 ### Description of changes ###: added dim dag calling for ecosys### Task/Ticket/Issue ###: NA
- Files Updated: infra/src/data_pipelines/z_dags/ecosys_project_xml_api_pipeline.py
infra/src/data_pipelines/z_dags/ecosys_xml_generic_pipeline.py

#####
## [tag-v-1.0.155] - 2025-04-09
### Deployed to:
- **qa**: 2025-04-09
### Changes:
- Deployed version tag-v-1.0.155
- Commit Message: RIL MIN Project ID Airflow vars capability ### Description of changes ###: RIL MIN Project ID Airflow vars capability### Task/Ticket/Issue ###: No Ticket
- Files Updated: infra/src/data_pipelines/0_sourcing/erm/jobs/erm_min_api_sourcing_job.py
infra/src/data_pipelines/z_dags/supply_chain_ril_min.py

#####
## [tag-v-1.0.164] - 2025-04-14
### Deployed to:
- **qa**: 2025-04-14
### Changes:
- Deployed version tag-v-1.0.164
- Commit Message: Added new schedule for Advisian and Comprimo pursuits ### Description of changes ###: Added new schedule for Advisian and Comprimo pursuits### Task/Ticket/Issue ###:N/A
- Files Updated: infra/src/data_pipelines/z_dags/genai_ped_generic_pipeline.py
infra/src/data_pipelines/z_dags/genai_sharepoint_gph_advisian_pipeline.py
infra/src/data_pipelines/z_dags/genai_sharepoint_gph_allotherpursuits_pipeline.py
infra/src/data_pipelines/z_dags/genai_sharepoint_gph_comprimo_pipeline.py
infra/src/data_pipelines/z_dags/genai_sharepoint_gph_gql_pipeline.py

#####
## [tag-v-1.0.165] - 2025-04-15
### Deployed to:
- **qa**: 2025-04-15
### Changes:
- Deployed version tag-v-1.0.165
- Commit Message: Update ecosys_project_xml_api_pipeline.py ### Description of changes ###:dag fixes### Task/Ticket/Issue ###: NA
- Files Updated: infra/src/data_pipelines/z_dags/ecosys_project_xml_api_pipeline.py

#####
## [tag-v-1.0.166] - 2025-04-15
### Deployed to:
- **qa**: 2025-04-15
### Changes:
- Deployed version tag-v-1.0.166
- Commit Message: Update aconex_cacdcwpspf06v_pipeline.py ### Description of changes ###: deploy to prod### Task/Ticket/Issue ###: NA
- Files Updated: infra/src/data_pipelines/z_dags/aconex_cacdcwpspf06v_pipeline.py

#####
## [tag-v-1.0.167] - 2025-04-16
### Deployed to:
- **qa**: 2025-04-16
### Changes:
- Deployed version tag-v-1.0.167
- Commit Message: added assurance datasets ### Description of changes ###: added assurance datasets### Task/Ticket/Issue ###: NA
- Files Updated: infra/src/data_pipelines/z_dags/assurance_csv_pipeline.py

#####
## [tag-v-1.0.170] - 2025-04-16
### Deployed to:
- **qa**: 2025-04-16
### Changes:
- Deployed version tag-v-1.0.170
- Commit Message: assurance QA deployment ### Description of changes ###: assurance QA deployment### Task/Ticket/Issue ###: NA
- Files Updated: infra/src/data_pipelines/z_dags/assurance_csv_pipeline.py

#####
## [tag-v-1.0.173] - 2025-04-16
### Deployed to:
- **qa**: 2025-04-16
### Changes:
- Deployed version tag-v-1.0.173
- Commit Message: assurance qa deployment ### Description of changes ###: assurance qa deployment### Task/Ticket/Issue ###: NA
- Files Updated: infra/src/data_pipelines/z_dags/assurance_csv_pipeline.py

#####
## [tag-v-1.0.181] - 2025-04-17
### Deployed to:
- **qa**: 2025-04-17
### Changes:
- Deployed version tag-v-1.0.181
- Commit Message: Feature/adt glue craw ### Description of changes ###:No code changes - QA migration### Task/Ticket/Issue ###:No code changes - QA migration
- Files Updated: infra/glue.tf
infra/glue_jobs.tf
infra/src/data_pipelines/0_sourcing/adt/jobs/adt_api_sourcing_job.py
infra/src/data_pipelines/z_dags/adt_sourcing_pipeline.py

#####
## [tag-v-1.0.183] - 2025-04-18
### Deployed to:
- **qa**: 2025-04-18
### Changes:
- Deployed version tag-v-1.0.183
- Commit Message: assurance DAG ### Description of changes ###: assurance DAG### Task/Ticket/Issue ###: NA
- Files Updated: infra/src/data_pipelines/z_dags/assurance_csv_pipeline.py

#####
## [tag-v-1.0.184] - 2025-04-18
### Deployed to:
- **qa**: 2025-04-18
### Changes:
- Deployed version tag-v-1.0.184
- Commit Message: ADT qa and prod deployment ### Description of changes ###: ADT qa and prod deployment### Task/Ticket/Issue ###: ADT qa and prod deployment
- Files Updated: infra/src/data_pipelines/z_dags/adt_sourcing_pipeline.py

#####
## [tag-v-1.0.188] - 2025-04-18
### Deployed to:
- **qa**: 2025-04-18
### Changes:
- Deployed version tag-v-1.0.188
- Commit Message: [Main] Update ecosys_project_xml_api_pipeline.py update task_id with schema_detect_{source_system_id}_{table}### Description of changes ###: Replaced detect_schema_change task_id with schema_detect_  .### Task/Ticket/Issue ###:Updated and replaced task_id in Dag for detect_schema_changes.
- Files Updated: infra/src/data_pipelines/z_dags/ecosys_project_xml_api_pipeline.py

#####
## [tag-v-1.0.189] - 2025-04-21
### Deployed to:
- **qa**: 2025-04-21
### Changes:
- Deployed version tag-v-1.0.189
- Commit Message: Update document_control_aconex_us_pipeline.py ### Description of changes ###: instance updated### Task/Ticket/Issue ###: NA
- Files Updated: infra/src/data_pipelines/z_dags/document_control_aconex_us_pipeline.py

#####
## [tag-v-1.0.190] - 2025-04-21
### Deployed to:
- **qa**: 2025-04-21
### Changes:
- Deployed version tag-v-1.0.190
- Commit Message: Satish athikari worley patch 1 ### Description of changes ###: update the changes in aconex bot### Task/Ticket/Issue ###: NA
- Files Updated: infra/src/data_pipelines/0_sourcing/ecosys/jobs/ecosys_api_xml_sourcing_job.py
infra/src/data_pipelines/z_dags/aconex_bot_euazrenc001v_pipeline.py
infra/src/data_pipelines/z_dags/aconex_bot_sgsdcwpenc02v_pipeline.py
infra/src/data_pipelines/z_dags/ecosys_project_xml_api_pipeline.py

#####
## [tag-v-1.0.191] - 2025-04-22
### Deployed to:
- **qa**: 2025-04-22
### Changes:
- Deployed version tag-v-1.0.191
- Commit Message: Feat/prd aconex schedule ### Description of changes ###: schedules for aconex### Task/Ticket/Issue ###: NA
- Files Updated: infra/src/data_pipelines/z_dags/aconex_ksa_pipeline.py
infra/src/data_pipelines/z_dags/aconex_ksa_users_pipeline.py
infra/src/data_pipelines/z_dags/aconex_uk_pipeline.py
infra/src/data_pipelines/z_dags/aconex_uk_users_pipeline.py
infra/src/data_pipelines/z_dags/aconex_us_pipeline.py
infra/src/data_pipelines/z_dags/aconex_us_users_pipeline.py
infra/src/data_pipelines/z_dags/ecosys_project_xml_api_pipeline.py

#####
## [tag-v-1.0.192] - 2025-04-22
### Deployed to:
- **qa**: 2025-04-22
### Changes:
- Deployed version tag-v-1.0.192
- Commit Message: Feature/schedule fixes ### Description of changes ###: fixes### Task/Ticket/Issue ###: NA
- Files Updated: infra/src/data_pipelines/z_dags/aconex_uk_users_pipeline.py
infra/src/data_pipelines/z_dags/ecosys_project_xml_api_pipeline.py

#####
## [tag-v-1.0.196] - 2025-04-23
### Deployed to:
- **qa**: 2025-04-23
### Changes:
- Deployed version tag-v-1.0.196
- Commit Message: Feature/aconex schedule ### Description of changes ###: Aconex schedule### Task/Ticket/Issue ###: NA
- Files Updated: infra/src/data_pipelines/z_dags/aconex_aus_pipeline.py
infra/src/data_pipelines/z_dags/aconex_aus_users_pipeline.py
infra/src/data_pipelines/z_dags/aconex_ca_pipeline.py
infra/src/data_pipelines/z_dags/aconex_ca_users_pipeline.py
infra/src/data_pipelines/z_dags/aconex_curation_pipeline.py
infra/src/data_pipelines/z_dags/aconex_hk_pipeline.py
infra/src/data_pipelines/z_dags/aconex_hk_users_pipeline.py

#####
## [tag-v-1.0.199] - 2025-04-23
### Deployed to:
- **qa**: 2025-04-23
### Changes:
- Deployed version tag-v-1.0.199
- Commit Message: Feature/aconex curation check ### Description of changes ###: schedule with single curation### Task/Ticket/Issue ###: NA
- Files Updated: infra/src/data_pipelines/z_dags/aconex_curation_pipeline.py
infra/src/data_pipelines/z_dags/aconex_ksa_pipeline.py
infra/src/data_pipelines/z_dags/aconex_uk_pipeline.py
infra/src/data_pipelines/z_dags/aconex_us_pipeline.py

#####
## [tag-v-1.0.200] - 2025-04-23
### Deployed to:
- **qa**: 2025-04-23
### Changes:
- Deployed version tag-v-1.0.200
- Commit Message: Feature/aconex schedule check ### Description of changes ###: schedule### Task/Ticket/Issue ###: NA
- Files Updated: infra/src/data_pipelines/z_dags/aconex_curation_pipeline.py
infra/src/data_pipelines/z_dags/aconex_ksa_pipeline.py
infra/src/data_pipelines/z_dags/aconex_uk_pipeline.py
infra/src/data_pipelines/z_dags/aconex_us_pipeline.py

#####
## [tag-v-1.0.201] - 2025-04-23
### Deployed to:
- **qa**: 2025-04-23
### Changes:
- Deployed version tag-v-1.0.201
- Commit Message: Update aconex_curation_pipeline.py ### Description of changes ###: fixes### Task/Ticket/Issue ###: NA
- Files Updated: infra/src/data_pipelines/z_dags/aconex_curation_pipeline.py

#####
## [tag-v-1.0.203] - 2025-04-23
### Deployed to:
- **qa**: 2025-04-23
### Changes:
- Deployed version tag-v-1.0.203
- Commit Message: Update aconex_hk_pipeline.py ### Description of changes ###: reschedule### Task/Ticket/Issue ###: NA
- Files Updated: infra/src/data_pipelines/z_dags/aconex_hk_pipeline.py

#####
## [tag-v-1.0.206] - 2025-04-23
### Deployed to:
- **qa**: 2025-04-23
### Changes:
- Deployed version tag-v-1.0.206
- Commit Message: Feature/ecosys project level ### Description of changes ###: add new dag for project specific for ecosys### Task/Ticket/Issue ###:NA
- Files Updated: infra/src/data_pipelines/z_dags/ecosys_project_specific_xml_api_pipeline.py
infra/src/data_pipelines/z_dags/ecosys_project_xml_api_pipeline.py

#####
## [tag-v-1.0.198] - 2025-04-23
### Deployed to:
- **qa**: 2025-04-23
### Changes:
- Deployed version tag-v-1.0.198
- Commit Message: Add files via upload ### Description of changes ###: e3d static file processing dag### Task/Ticket/Issue ###: e3d static file processing dag
- Files Updated: infra/src/data_pipelines/z_dags/e3d_roc_status_static_file_pipeline.py

#####
## [tag-v-1.0.208] - 2025-04-23
### Deployed to:
- **qa**: 2025-04-23
### Changes:
- Deployed version tag-v-1.0.208
- Commit Message: Feature/ecosys dag ### Description of changes ###:updated ecosys_xml dag file### Task/Ticket/Issue ###:NA
- Files Updated: infra/src/data_pipelines/z_dags/ecosys_project_specific_xml_api_pipeline.py
infra/src/data_pipelines/z_dags/ecosys_project_xml_api_pipeline.py

#####
## [tag-v-1.0.211] - 2025-04-24
### Deployed to:
- **qa**: 2025-04-24
### Changes:
- Deployed version tag-v-1.0.211
- Commit Message: Update ecosys_project_xml_api_pipeline.py ### Description of changes ###: redeploy to prod### Task/Ticket/Issue ###: NA
- Files Updated: infra/src/data_pipelines/z_dags/ecosys_project_xml_api_pipeline.py

#####
## [tag-v-1.0.212] - 2025-04-24
### Deployed to:
- **qa**: 2025-04-24
### Changes:
- Deployed version tag-v-1.0.212
- Commit Message: Feature/prd scheduler ### Description of changes ###: schedule and ecosys crawler finish### Task/Ticket/Issue ###: NA
- Files Updated: infra/src/data_pipelines/z_dags/aconex_aus_pipeline.py
infra/src/data_pipelines/z_dags/aconex_ca_pipeline.py
infra/src/data_pipelines/z_dags/aconex_cn_pipeline.py
infra/src/data_pipelines/z_dags/aconex_hk_pipeline.py
infra/src/data_pipelines/z_dags/aconex_ksa_pipeline.py
infra/src/data_pipelines/z_dags/aconex_us_pipeline.py
infra/src/data_pipelines/z_dags/ecosys_project_specific_xml_api_pipeline.py
infra/src/data_pipelines/z_dags/ecosys_project_xml_api_pipeline.py

#####
## [tag-v-1.0.217] - 2025-04-24
### Deployed to:
- **qa**: 2025-04-24
### Changes:
- Deployed version tag-v-1.0.217
- Commit Message: Update aconex_mea_users_pipeline.py ### Description of changes ###: mea- schedule### Task/Ticket/Issue ###: NA
- Files Updated: infra/src/data_pipelines/z_dags/aconex_mea_users_pipeline.py

#####
## [tag-v-1.0.218] - 2025-04-24
### Deployed to:
- **qa**: 2025-04-24
### Changes:
- Deployed version tag-v-1.0.218
- Commit Message: Feature/ecosys proj fixes ### Description of changes ###: fixes for schema### Task/Ticket/Issue ###: NA
- Files Updated: infra/src/data_pipelines/z_dags/ecosys_project_specific_xml_api_pipeline.py
infra/src/data_pipelines/z_dags/ecosys_project_xml_api_pipeline.py

#####
## [tag-v-1.0.226] - 2025-04-29
### Deployed to:
- **qa**: 2025-04-29
### Changes:
- Deployed version tag-v-1.0.226
- Commit Message: Updated schedule ### Description of changes ###:Updated schedule### Task/Ticket/Issue ###:Updated schedule
- Files Updated: infra/src/data_pipelines/z_dags/assurance_csv_pipeline.py
