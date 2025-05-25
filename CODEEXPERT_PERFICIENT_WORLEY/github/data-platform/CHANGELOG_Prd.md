
#####
## [tag-v0.0.80] - 2025-01-10
### Deployed to:
- **prd**: 2025-01-10
### Changes:
- Deployed version tag-v0.0.80
- Commit Message: Feature/ecosys initialload to2years prod ### Description of changes ###: Ecosys Increased the prod data for 2 years### Files Changed ###: ecosys_generic_sourcing.py, ecosys_generic_pipeline.py### Reason for Change ###: As per the requirement prod needs 2 years data per confirmation from Vikash### Task/Ticket/Issue ###:Ecosys prod changes and deployment - https://dev.azure.com/WorleyDevelopment/AWS%20Transformation/_workitems/edit/52972
- Files Updated: infra/glue_jobs.tf
infra/src/data_pipelines/0_sourcing/ecosys/jobs/ecosys_api_sourcing_job.py
infra/src/data_pipelines/z_dags/ecosys_generic_pipeline.py

#####
## [tag-v0.0.85] - 2025-01-10
### Deployed to:
- **prd**: 2025-01-10
### Changes:
- Deployed version tag-v0.0.85
- Commit Message: Updated CSP scripts ### Description of changes ###:Updated CSP code to include full and delta### Files Changed ###:3 files are updated### Reason for Change ###:Updated CSP code to include full and delta### Task/Ticket/Issue ###:No ticket
- Files Updated: infra/src/data_pipelines/0_sourcing/csp_salesforce/csp_salesforce_job.py
infra/src/data_pipelines/z_dags/csp_salesforce_delta_pipeline.py
infra/src/data_pipelines/z_dags/csp_salesforce_full_pipeline.py

#####
## [tag-v0.0.87] - 2025-01-13
### Deployed to:
- **prd**: 2025-01-13
### Changes:
- Deployed version tag-v0.0.87
- Commit Message: Feature/csp updates ### Description of changes ###:Updated aurora sql job and csp schedulder### Files Changed ###: aurora glue job and csp delta scheduler### Reason for Change ###:Enhancement### Task/Ticket/Issue ###:No ticket
- Files Updated: infra/src/data_pipelines/0_sourcing/aurora/iceberg_aurora_ingestion.py
infra/src/data_pipelines/z_dags/csp_salesforce_delta_pipeline.py

#####
## [tag-v0.0.89] - 2025-01-13
### Deployed to:
- **prd**: 2025-01-13
### Changes:
- Deployed version tag-v0.0.89
- Commit Message: Update oracle_gbs_csv_pipeline.py ### Description of changes ###: GBS currency entities to be enabled in prod### Files Changed ###: oracle_gbs_csv_pipeline.py### Reason for Change ###: enabling schedule### Task/Ticket/Issue ###: ADO task 52945
- Files Updated: infra/src/data_pipelines/z_dags/oracle_gbs_csv_pipeline.py

#####
## [tag-v0.0.92] - 2025-01-15
### Deployed to:
- **prd**: 2025-01-15
### Changes:
- Deployed version tag-v0.0.92
- Commit Message: Update oracle_gbs_csv_pipeline.py ### Description of changes ###: change the schedule of GBS r2 dag### Files Changed ###:oracle_gbs_csv_pipeline.py### Reason for Change ###: change the schedule of GBS r2 dag### Task/Ticket/Issue ###: ADO 52945
- Files Updated: infra/src/data_pipelines/z_dags/oracle_gbs_csv_pipeline.py

#####
## [tag-v0.0.93] - 2025-01-16
### Deployed to:
- **prd**: 2025-01-16
### Changes:
- Deployed version tag-v0.0.93
- Commit Message: Updated csp dag for crawler sensor ### Description of changes ###: Added crawler sensor### Files Changed ###: Daily and Delta csp dag### Reason for Change ###:Enhancement### Task/Ticket/Issue ###:N/A
- Files Updated: infra/src/data_pipelines/z_dags/csp_salesforce_delta_pipeline.py
infra/src/data_pipelines/z_dags/csp_salesforce_full_pipeline.py

#####
## [tag-v0.0.95] - 2025-01-16
### Deployed to:
- **prd**: 2025-01-16
### Changes:
- Deployed version tag-v0.0.95
- Commit Message: update to crawler sensor ### Description of changes ###: Updated crawler sensor### Files Changed ###: Delta and Daily Dag### Reason for Change ###: Crawler sensor### Task/Ticket/Issue ###:NA
- Files Updated: infra/src/data_pipelines/z_dags/csp_salesforce_delta_pipeline.py
infra/src/data_pipelines/z_dags/csp_salesforce_full_pipeline.py

#####
## [tag-v-1.0.4] - 2025-01-17
### Deployed to:
- **prd**: 2025-01-17
### Changes:
- Deployed version tag-v-1.0.4
- Commit Message: no changes just erm dag schedule change ### Description of changes ###: DAG Schedule for ERM to be effective on higher environment### Files Changed ###:erm_sourcing_pipeline.py### Reason for Change ###:from daily once to daily 5.30 AM IST### Task/Ticket/Issue ###:NA
- Files Updated: infra/src/data_pipelines/z_dags/erm_sourcing_pipeline.py

#####
## [tag-v-1.0.28] - 2025-01-24
### Deployed to:
- **prd**: 2025-01-24
### Changes:
- Deployed version tag-v-1.0.28
- Commit Message: Update circuit_breaker_dag.py ### Description of changes ###:Circuit-dag### Files Changed ###:find below### Reason for Change ###:Circuit dag### Task/Ticket/Issue ###:no ticket
- Files Updated: infra/src/data_pipelines/z_dags/circuit_breaker_dag.py

#####
## [tag-v-1.0.29] - 2025-01-24
### Deployed to:
- **prd**: 2025-01-24
### Changes:
- Deployed version tag-v-1.0.29
- Commit Message: Feature/ecosys prod depl ### Description of changes ###:prod deploment### Files Changed ###:ecosys_xml_dag### Reason for Change ###:prod deployment### Task/Ticket/Issue ###:https://dev.azure.com/WorleyDevelopment/AWS%20Transformation/_workitems/edit/52972/
- Files Updated: infra/src/data_pipelines/z_dags/ecosys_project_xml_api_pipeline.py
infra/src/data_pipelines/z_dags/ecosys_xml_generic_pipeline.py

#####
## [tag-v-1.0.19] - 2025-01-28
### Deployed to:
- **prd**: 2025-01-28
### Changes:
- Deployed version tag-v-1.0.19
- Commit Message: Feature/aconex project update ### Description of changes ###:Added new project### Files Changed ###:DAC files for Aconex UK and KSA### Reason for Change ###:Request for new project### Task/Ticket/Issue ###:N/A
- Files Updated: infra/src/data_pipelines/z_dags/document_control_aconex_ksa1_pipeline.py
infra/src/data_pipelines/z_dags/document_control_aconex_uk_pipeline.py
infra/src/data_pipelines/z_dags/oracle_p6_pipeline_dac_v2.py

#####
## [tag-v-1.0.32] - 2025-01-28
### Deployed to:
- **prd**: 2025-01-28
### Changes:
- Deployed version tag-v-1.0.32
- Commit Message: Feature/gbs dag to prod ### Description of changes ###: Taking GBS dags to prod### Files Changed ###: oracle_gbs_csv_pipeline.py, oracle_gbs_r4_csv_pipeline.py### Reason for Change ###: Taking GBS dags to prod### Task/Ticket/Issue ###: ADO 53202 and 53203
- Files Updated: infra/src/data_pipelines/z_dags/oracle_gbs_csv_pipeline.py
infra/src/data_pipelines/z_dags/oracle_gbs_r4_csv_pipeline.py

#####
## [tag-v-1.0.33] - 2025-01-28
### Deployed to:
- **prd**: 2025-01-28
### Changes:
- Deployed version tag-v-1.0.33
- Commit Message: added logic to stop next run when current run has failed ### Description of changes ###:added logic to stop next run when current run has failed### Files Changed ###:added logic to stop next run when current run has failed### Reason for Change ###:added logic to stop next run when current run has failed### Task/Ticket/Issue ###:added logic to stop next run when current run has failed
- Files Updated: infra/src/data_pipelines/z_dags/erm_sourcing_pipeline.py

#####
## [tag-v-1.0.35] - 2025-01-28
### Deployed to:
- **prd**: 2025-01-28
### Changes:
- Deployed version tag-v-1.0.35
- Commit Message: Feature/genai notification fix ### Description of changes ###:GenAI schedule change### Files Changed ###:GenAI ped dag### Reason for Change ###:Enhancement### Task/Ticket/Issue ###:N/A
- Files Updated: infra/src/data_pipelines/0_sourcing/genai/ped_dataverse_job.py
infra/src/data_pipelines/0_sourcing/genai/sharepoint_job.py
infra/src/data_pipelines/z_dags/genai_ped_generic_pipeline.py

#####
## [tag-v-1.0.34] - 2025-01-29
### Deployed to:
- **prd**: 2025-01-29
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
- **prd**: 2025-01-29
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
- **prd**: 2025-01-30
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
- **prd**: 2025-01-30
### Changes:
- Deployed version tag-v-1.0.41
- Commit Message: Update circuit_breaker_dag.py ### Description of changes ###:circuit_breaker_dag.py### Files Changed ###:circuit_breaker_dag.py### Reason for Change ###:circuit_breaker_dag.py### Task/Ticket/Issue ###:circuit_breaker_dag.py
- Files Updated: infra/src/data_pipelines/z_dags/circuit_breaker_dag.py

#####
## [tag-v-1.0.42] - 2025-02-03
### Deployed to:
- **prd**: 2025-02-03
### Changes:
- Deployed version tag-v-1.0.42
- Commit Message: Feature/gbs r4 schedule change ### Description of changes ###: Changes to r4 dag schedule and sftp csv file### Files Changed ###: sftp_sourcing_job.py,  oracle_gbs_r4_csv_pipeline.py### Reason for Change ###: after confirming with support, changing r4 schedule to 1 PM### Task/Ticket/Issue ###: ADO 53202
- Files Updated: infra/src/data_pipelines/0_sourcing/sftp/sftp_sourcing_job.py
infra/src/data_pipelines/z_dags/oracle_gbs_r4_csv_pipeline.py

#####
## [tag-v-1.0.52] - 2025-02-05
### Deployed to:
- **prd**: 2025-02-05
### Changes:
- Deployed version tag-v-1.0.52
- Commit Message: Updated ecodata schedule ### Description of changes ###:Updated DAG schedule for Ecosys### Files Changed ###:Ecosys DAG### Reason for Change ###:Requirement### Task/Ticket/Issue ###:N/A
- Files Updated: infra/src/data_pipelines/z_dags/ecosys_xml_generic_pipeline.py

#####
## [tag-v-1.0.57] - 2025-02-06
### Deployed to:
- **prd**: 2025-02-06
### Changes:
- Deployed version tag-v-1.0.57
- Commit Message: Added new DAC to handle O3 initial load ### Description of changes ###:Added new dag for O3 DAC### Files Changed ###:New schedule### Reason for Change ###:New load### Task/Ticket/Issue ###:N/A
- Files Updated: infra/src/data_pipelines/z_dags/o3_pipeline_dac.py

#####
## [tag-v-1.0.62] - 2025-02-08
### Deployed to:
- **prd**: 2025-02-08
### Changes:
- Deployed version tag-v-1.0.62
- Commit Message: Feature/aconex userrole schema ### Description of changes ###: Removed the userrole and updated the schedules for each region at midnight### Files Changed ###: aconex_us_pipeline.py### Reason for Change ###: change request### Task/Ticket/Issue ###: Adhoc task
- Files Updated: infra/src/data_pipelines/z_dags/aconex_ksa_pipeline.py
infra/src/data_pipelines/z_dags/aconex_uk_pipeline.py
infra/src/data_pipelines/z_dags/aconex_us_pipeline.py

#####
## [tag-v-1.0.64] - 2025-02-10
### Deployed to:
- **prd**: 2025-02-10
### Changes:
- Deployed version tag-v-1.0.64
- Commit Message: Feature/acnx dag ### Description of changes ###:new changes of schedule### Files Changed ###:aconex_us_pipeline.py, aconex_uk_pipeline.py### Reason for Change ###: schedule change### Task/Ticket/Issue ###:https://dev.azure.com/WorleyDevelopment/AWS%20Transformation/_workitems/edit/52936
- Files Updated: infra/src/data_pipelines/z_dags/aconex_ksa_pipeline.py
infra/src/data_pipelines/z_dags/aconex_uk_pipeline.py
infra/src/data_pipelines/z_dags/aconex_us_pipeline.py
infra/src/data_pipelines/z_dags/ecosys_xml_generic_pipeline.py

#####
## [tag-v-1.0.75] - 2025-02-12
### Deployed to:
- **prd**: 2025-02-12
### Changes:
- Deployed version tag-v-1.0.75
- Commit Message: Update hexagon_US_pipeline.py ### Description of changes ###: correcting the crawler sensor### Files Changed ###:hexagon_US_pipeline.py### Reason for Change ###:correcting the crawler### Task/Ticket/Issue ###:https://dev.azure.com/WorleyDevelopment/AWS%20Transformation/_workitems/edit/53764
- Files Updated: infra/src/data_pipelines/z_dags/hexagon_US_pipeline.py

#####
## [tag-v-1.0.73] - 2025-02-12
### Deployed to:
- **prd**: 2025-02-12
### Changes:
- Deployed version tag-v-1.0.73
- Commit Message: Update hexagon_US_pipeline.py ### Description of changes ###: added more control### Files Changed ###:hexagon_US_pipeline.py### Reason for Change ###: enhancement### Task/Ticket/Issue ###:https://dev.azure.com/WorleyDevelopment/AWS%20Transformation/_workitems/edit/53764
- Files Updated: infra/src/data_pipelines/z_dags/hexagon_US_pipeline.py

#####
## [tag-v-1.0.76] - 2025-02-12
### Deployed to:
- **prd**: 2025-02-12
### Changes:
- Deployed version tag-v-1.0.76
- Commit Message: updated delta schedule ### Description of changes ###:Update to CSP delta schedule### Files Changed ###:CSP Delta schedule### Reason for Change ###:To avoid conflict with full load### Task/Ticket/Issue ###:N/A
- Files Updated: infra/src/data_pipelines/z_dags/csp_salesforce_delta_pipeline.py

#####
## [tag-v-1.0.85] - 2025-02-14
### Deployed to:
- **prd**: 2025-02-14
### Changes:
- Deployed version tag-v-1.0.85
- Commit Message: Updated DAG for delta ### Description of changes ###:DAG Update for CSP### Files Changed ###:Update to schedule### Reason for Change ###:To avoid clash of Full and Delta### Task/Ticket/Issue ###:N/A
- Files Updated: infra/src/data_pipelines/z_dags/csp_salesforce_delta_pipeline.py

#####
## [tag-v-1.0.105] - 2025-02-19
### Deployed to:
- **prd**: 2025-02-19
### Changes:
- Deployed version tag-v-1.0.105
- Commit Message: Updated CSP schedule ### Description of changes ###:CSP Dag update### Task/Ticket/Issue ###:N/A
- Files Updated: infra/src/data_pipelines/z_dags/csp_salesforce_delta_pipeline.py

#####
## [tag-v-1.0.115] - 2025-02-25
### Deployed to:
- **prd**: 2025-02-25
### Changes:
- Deployed version tag-v-1.0.115
- Commit Message: Update to the csp pipeline ### Description of changes ###:Updated csp pipeline### Task/Ticket/Issue ###:N/A
- Files Updated: infra/src/data_pipelines/z_dags/csp_salesforce_delta_pipeline.py
infra/src/data_pipelines/z_dags/csp_salesforce_full_pipeline.py

#####
## [tag-v-1.0.116] - 2025-02-25
### Deployed to:
- **prd**: 2025-02-25
### Changes:
- Deployed version tag-v-1.0.116
- Commit Message: updated csp pipeline ### Description of changes ###:Updates CSP pipeline### Task/Ticket/Issue ###:N/A
- Files Updated: infra/src/data_pipelines/z_dags/csp_salesforce_curation_pipeline.py
infra/src/data_pipelines/z_dags/csp_salesforce_delta_pipeline.py
infra/src/data_pipelines/z_dags/csp_salesforce_full_pipeline.py

#####
## [tag-v-1.0.112] - 2025-02-26
### Deployed to:
- **prd**: 2025-02-26
### Changes:
- Deployed version tag-v-1.0.112
- Commit Message: Feature/r2 dag fix ### Description of changes ###: curation job upgraded version and r2 dag fix### Task/Ticket/Issue ###: GBS incremental load fix
- Files Updated: infra/glue_jobs.tf
infra/src/data_pipelines/z_dags/oracle_gbs_csv_pipeline.py

#####
## [tag-v-1.0.119] - 2025-02-28
### Deployed to:
- **prd**: 2025-02-28
### Changes:
- Deployed version tag-v-1.0.119
- Commit Message: added retry and updated schedule ### Description of changes ###:added retry and updated schedule### Task/Ticket/Issue ###:added retry and updated schedule
- Files Updated: infra/src/data_pipelines/z_dags/erm_sourcing_pipeline.py

#####
## [tag-v-1.0.123] - 2025-03-03
### Deployed to:
- **prd**: 2025-03-03
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
## [tag-v-1.0.126] - 2025-03-04
### Deployed to:
- **prd**: 2025-03-04
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
## [tag-v-1.0.139] - 2025-03-11
### Deployed to:
- **prd**: 2025-03-11
### Changes:
- Deployed version tag-v-1.0.139
- Commit Message: changed the schedule time for share point gsb finance ### Description of changes ###: DAG scheduler timing from 10th 9:30 to11th 8:30### Task/Ticket/Issue ###:53789
- Files Updated: infra/src/data_pipelines/z_dags/sharepoint_gbs_pipeline.py

#####
## [tag-v-1.0.147] - 2025-03-25
### Deployed to:
- **prd**: 2025-03-25
### Changes:
- Deployed version tag-v-1.0.147
- Commit Message: updated schedule for PED and CSP to aest ### Description of changes ###:Update to PED and CSP schedule### Task/Ticket/Issue ###:N/A
- Files Updated: infra/src/data_pipelines/z_dags/csp_salesforce_delta_pipeline.py
infra/src/data_pipelines/z_dags/csp_salesforce_full_pipeline.py
infra/src/data_pipelines/z_dags/genai_ped_generic_pipeline.py

#####
## [tag-v-1.0.148] - 2025-03-28
### Deployed to:
- **prd**: 2025-03-28
### Changes:
- Deployed version tag-v-1.0.148
- Commit Message: Feature/e3d dag changes ### Description of changes ###: Change in E3D MTO and ADO dag timings to avoid IICS conflict### Task/Ticket/Issue ###: Change in E3D MTO and ADO dag timings to avoid IICS conflict
- Files Updated: infra/src/data_pipelines/z_dags/e3d_ado_pipeline.py
infra/src/data_pipelines/z_dags/e3d_mto_pipeline.py

#####
## [tag-v-1.0.155] - 2025-04-09
### Deployed to:
- **prd**: 2025-04-09
### Changes:
- Deployed version tag-v-1.0.155
- Commit Message: RIL MIN Project ID Airflow vars capability ### Description of changes ###: RIL MIN Project ID Airflow vars capability### Task/Ticket/Issue ###: No Ticket
- Files Updated: infra/src/data_pipelines/0_sourcing/erm/jobs/erm_min_api_sourcing_job.py
infra/src/data_pipelines/z_dags/supply_chain_ril_min.py

#####
## [tag-v-1.0.164] - 2025-04-14
### Deployed to:
- **prd**: 2025-04-14
### Changes:
- Deployed version tag-v-1.0.164
- Commit Message: Added new schedule for Advisian and Comprimo pursuits ### Description of changes ###: Added new schedule for Advisian and Comprimo pursuits### Task/Ticket/Issue ###:N/A
- Files Updated: infra/src/data_pipelines/z_dags/genai_ped_generic_pipeline.py
infra/src/data_pipelines/z_dags/genai_sharepoint_gph_advisian_pipeline.py
infra/src/data_pipelines/z_dags/genai_sharepoint_gph_allotherpursuits_pipeline.py
infra/src/data_pipelines/z_dags/genai_sharepoint_gph_comprimo_pipeline.py
infra/src/data_pipelines/z_dags/genai_sharepoint_gph_gql_pipeline.py

#####
## [tag-v-1.0.166] - 2025-04-15
### Deployed to:
- **prd**: 2025-04-15
### Changes:
- Deployed version tag-v-1.0.166
- Commit Message: Update aconex_cacdcwpspf06v_pipeline.py ### Description of changes ###: deploy to prod### Task/Ticket/Issue ###: NA
- Files Updated: infra/src/data_pipelines/z_dags/aconex_cacdcwpspf06v_pipeline.py

#####
## [tag-v-1.0.183] - 2025-04-18
### Deployed to:
- **prd**: 2025-04-18
### Changes:
- Deployed version tag-v-1.0.183
- Commit Message: assurance DAG ### Description of changes ###: assurance DAG### Task/Ticket/Issue ###: NA
- Files Updated: infra/src/data_pipelines/z_dags/assurance_csv_pipeline.py

#####
## [tag-v-1.0.184] - 2025-04-18
### Deployed to:
- **prd**: 2025-04-18
### Changes:
- Deployed version tag-v-1.0.184
- Commit Message: ADT qa and prod deployment ### Description of changes ###: ADT qa and prod deployment### Task/Ticket/Issue ###: ADT qa and prod deployment
- Files Updated: infra/src/data_pipelines/z_dags/adt_sourcing_pipeline.py

#####
## [tag-v-1.0.188] - 2025-04-18
### Deployed to:
- **prd**: 2025-04-18
### Changes:
- Deployed version tag-v-1.0.188
- Commit Message: [Main] Update ecosys_project_xml_api_pipeline.py update task_id with schema_detect_{source_system_id}_{table}### Description of changes ###: Replaced detect_schema_change task_id with schema_detect_  .### Task/Ticket/Issue ###:Updated and replaced task_id in Dag for detect_schema_changes.
- Files Updated: infra/src/data_pipelines/z_dags/ecosys_project_xml_api_pipeline.py

#####
## [tag-v-1.0.189] - 2025-04-21
### Deployed to:
- **prd**: 2025-04-21
### Changes:
- Deployed version tag-v-1.0.189
- Commit Message: Update document_control_aconex_us_pipeline.py ### Description of changes ###: instance updated### Task/Ticket/Issue ###: NA
- Files Updated: infra/src/data_pipelines/z_dags/document_control_aconex_us_pipeline.py

#####
## [tag-v-1.0.190] - 2025-04-21
### Deployed to:
- **prd**: 2025-04-21
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
- **prd**: 2025-04-22
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
- **prd**: 2025-04-22
### Changes:
- Deployed version tag-v-1.0.192
- Commit Message: Feature/schedule fixes ### Description of changes ###: fixes### Task/Ticket/Issue ###: NA
- Files Updated: infra/src/data_pipelines/z_dags/aconex_uk_users_pipeline.py
infra/src/data_pipelines/z_dags/ecosys_project_xml_api_pipeline.py

#####
## [tag-v-1.0.196] - 2025-04-23
### Deployed to:
- **prd**: 2025-04-23
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
## [tag-v-1.0.200] - 2025-04-23
### Deployed to:
- **prd**: 2025-04-23
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
- **prd**: 2025-04-23
### Changes:
- Deployed version tag-v-1.0.201
- Commit Message: Update aconex_curation_pipeline.py ### Description of changes ###: fixes### Task/Ticket/Issue ###: NA
- Files Updated: infra/src/data_pipelines/z_dags/aconex_curation_pipeline.py

#####
## [tag-v-1.0.206] - 2025-04-24
### Deployed to:
- **prd**: 2025-04-24
### Changes:
- Deployed version tag-v-1.0.206
- Commit Message: Feature/ecosys project level ### Description of changes ###: add new dag for project specific for ecosys### Task/Ticket/Issue ###:NA
- Files Updated: infra/src/data_pipelines/z_dags/ecosys_project_specific_xml_api_pipeline.py
infra/src/data_pipelines/z_dags/ecosys_project_xml_api_pipeline.py

#####
## [tag-v-1.0.209] - 2025-04-24
### Deployed to:
- **prd**: 2025-04-24
### Changes:
- Deployed version tag-v-1.0.209
- Commit Message: Feature/aconex instance ### Description of changes ###: aconex instance schedule### Task/Ticket/Issue ###: NA
- Files Updated: infra/src/data_pipelines/z_dags/aconex_ca_pipeline.py
infra/src/data_pipelines/z_dags/aconex_cn_pipeline.py
infra/src/data_pipelines/z_dags/aconex_cn_users_pipeline.py
infra/src/data_pipelines/z_dags/aconex_mea_pipeline.py
infra/src/data_pipelines/z_dags/aconex_mea_users_pipeline.py

#####
## [tag-v-1.0.211] - 2025-04-24
### Deployed to:
- **prd**: 2025-04-24
### Changes:
- Deployed version tag-v-1.0.211
- Commit Message: Update ecosys_project_xml_api_pipeline.py ### Description of changes ###: redeploy to prod### Task/Ticket/Issue ###: NA
- Files Updated: infra/src/data_pipelines/z_dags/ecosys_project_xml_api_pipeline.py

#####
## [tag-v-1.0.212] - 2025-04-24
### Deployed to:
- **prd**: 2025-04-24
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
- **prd**: 2025-04-24
### Changes:
- Deployed version tag-v-1.0.217
- Commit Message: Update aconex_mea_users_pipeline.py ### Description of changes ###: mea- schedule### Task/Ticket/Issue ###: NA
- Files Updated: infra/src/data_pipelines/z_dags/aconex_mea_users_pipeline.py

#####
## [tag-v-1.0.218] - 2025-04-24
### Deployed to:
- **prd**: 2025-04-24
### Changes:
- Deployed version tag-v-1.0.218
- Commit Message: Feature/ecosys proj fixes ### Description of changes ###: fixes for schema### Task/Ticket/Issue ###: NA
- Files Updated: infra/src/data_pipelines/z_dags/ecosys_project_specific_xml_api_pipeline.py
infra/src/data_pipelines/z_dags/ecosys_project_xml_api_pipeline.py

#####
## [tag-v-1.0.224] - 2025-04-28
### Deployed to:
- **prd**: 2025-04-28
### Changes:
- Deployed version tag-v-1.0.224
- Commit Message: Update adt_sourcing_pipeline.py ### Description of changes ###: Added schedule### Task/Ticket/Issue ###: Added schedule
- Files Updated: infra/src/data_pipelines/z_dags/adt_sourcing_pipeline.py

#####
## [tag-v-1.0.226] - 2025-04-29
### Deployed to:
- **prd**: 2025-04-29
### Changes:
- Deployed version tag-v-1.0.226
- Commit Message: Updated schedule ### Description of changes ###:Updated schedule### Task/Ticket/Issue ###:Updated schedule
- Files Updated: infra/src/data_pipelines/z_dags/assurance_csv_pipeline.py
