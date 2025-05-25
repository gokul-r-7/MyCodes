
#####
## [tag-v0.0.0] - 2025-03-20
### Deployed to:
- **qa**: 2025-03-20
### Changes:
- Deployed version tag-v0.0.0
- Commit Message: Feature/precision fix ### Description of changes ###: changing e3d_pipe support model and project_code length change### Task/Ticket/Issue ###: ADO task 54258
- Files Updated: src/engineering_domain_model/models/e3d_pipe_support_attr.sql
src/engineering_domain_model/models/schema.yml

#####
## [tag-v0.0.1] - 2025-03-24
### Deployed to:
- **qa**: 2025-03-24
### Changes:
- Deployed version tag-v0.0.1
- Commit Message: Feature/transform table prefix ### Description of changes ###: Adding transform_prefix to all transformed tables. ### Task/Ticket/Issue ###: ADO task 54258
- Files Updated: src/engineering_domain_model/models/schema.yml
src/engineering_domain_model/models/transformed_audit_table.sql
src/engineering_domain_model/models/transformed_curated_roc_status_projectname.sql
src/engineering_domain_model/models/transformed_dim_memstatus.sql
src/engineering_domain_model/models/transformed_dim_project.sql
src/engineering_domain_model/models/transformed_e3d_pipe_support_attr.sql
src/engineering_domain_model/models/transformed_e3d_pipes.sql
src/engineering_domain_model/models/transformed_e3d_pipes_mto_comps.sql
src/engineering_domain_model/models/transformed_e3d_wbs_cwa_cwpzone.sql

#####
## [tag-v0.0.2] - 2025-03-24
### Deployed to:
- **qa**: 2025-03-24
### Changes:
- Deployed version tag-v0.0.2
- Commit Message: Feature/transform table change ### Description of changes ###: change trasnformed_layer model name for roc status### Task/Ticket/Issue ###: ADO 54258
- Files Updated: src/engineering_domain_model/models/schema.yml
src/engineering_domain_model/models/transformed_roc_status_projectname.sql

#####
## [tag-v0.0.5] - 2025-04-02
### Deployed to:
- **qa**: 2025-04-02
### Changes:
- Deployed version tag-v0.0.5
- Commit Message: Feature/r2 deployment ### Description of changes ###: Release 2 models deployment to qa### Task/Ticket/Issue ###: 54773
- Files Updated: src/engineering_domain_model/models/schema.yml
src/engineering_domain_model/models/sources.yml
src/engineering_domain_model/models/transformed_audit_table.sql
src/engineering_domain_model/models/transformed_dim_memstatus.sql
src/engineering_domain_model/models/transformed_dim_project.sql
src/engineering_domain_model/models/transformed_e3d_elec_mto_equip.sql
src/engineering_domain_model/models/transformed_e3d_elec_mto_tray.sql
src/engineering_domain_model/models/transformed_e3d_equip.sql
src/engineering_domain_model/models/transformed_e3d_gensec_floor_union.sql
src/engineering_domain_model/models/transformed_e3d_pipe_support_attr.sql
src/engineering_domain_model/models/transformed_e3d_pipes.sql
src/engineering_domain_model/models/transformed_e3d_pipes_mto_comps.sql
src/engineering_domain_model/models/transformed_e3d_struc.sql
src/engineering_domain_model/models/transformed_e3d_wbs_cwa_cwpzone.sql
src/engineering_domain_model/models/transformed_me_function_code.sql
src/engineering_domain_model/models/transformed_roc_status_projectname.sql
src/engineering_domain_model/models/transformed_spel_codelists.sql
src/engineering_domain_model/models/transformed_spel_modelitem.sql
src/engineering_domain_model/models/transformed_spel_plantitem.sql
src/engineering_domain_model/models/transformed_spel_t_cable.sql
src/engineering_domain_model/models/transformed_spel_t_equipments.sql

#####
## [tag-v0.0.6] - 2025-04-02
### Deployed to:
- **qa**: 2025-04-02
### Changes:
- Deployed version tag-v0.0.6
- Commit Message: Feature/r2 fix ### Description of changes ###: Release 2 deployment to qa### Task/Ticket/Issue ###: 54773
- Files Updated: src/engineering_domain_model/models/schema.yml
src/engineering_domain_model/models/transformed_spel_t_cable.sql

#####
## [tag-v0.0.16] - 2025-04-07
### Deployed to:
- **qa**: 2025-04-07
### Changes:
- Deployed version tag-v0.0.16
- Commit Message: qa migration ### Description of changes ###: qa deployment of all v2 models. ### Task/Ticket/Issue ###: 53760
- Files Updated: src/engineering_domain_model/dbt_project.yml
src/engineering_domain_model/models/schema.yml
src/engineering_domain_model/models/sources.yml
src/engineering_domain_model/models/transformed_ado_modelreviewitemstracker.sql
src/engineering_domain_model/models/transformed_aim_tags_pwrbi.sql
src/engineering_domain_model/models/transformed_audit_table.sql
src/engineering_domain_model/models/transformed_dim_memstatus.sql
src/engineering_domain_model/models/transformed_dim_project.sql
src/engineering_domain_model/models/transformed_e3d_elec_mto_equip.sql
src/engineering_domain_model/models/transformed_e3d_elec_mto_tray.sql
src/engineering_domain_model/models/transformed_e3d_equip.sql
src/engineering_domain_model/models/transformed_e3d_gensec_floor_union.sql
src/engineering_domain_model/models/transformed_e3d_pipe_support_attr.sql
src/engineering_domain_model/models/transformed_e3d_pipes.sql
src/engineering_domain_model/models/transformed_e3d_pipes_mto_comps.sql
src/engineering_domain_model/models/transformed_e3d_pipes_mto_lists.sql
src/engineering_domain_model/models/transformed_e3d_struc.sql
src/engineering_domain_model/models/transformed_e3d_wbs_cwa_cwpzone.sql
src/engineering_domain_model/models/transformed_engreg_assumptions_index.sql
src/engineering_domain_model/models/transformed_engreg_hold_index.sql
src/engineering_domain_model/models/transformed_engreg_seal_decision.sql
src/engineering_domain_model/models/transformed_engreg_td_index.sql
src/engineering_domain_model/models/transformed_isotracker_stress_calc.sql
src/engineering_domain_model/models/transformed_isotracker_union.sql
src/engineering_domain_model/models/transformed_me_function_code.sql
src/engineering_domain_model/models/transformed_mel_cp2_mechanical_eqip.sql
src/engineering_domain_model/models/transformed_mps_logs_all.sql
src/engineering_domain_model/models/transformed_omie_combo.sql
src/engineering_domain_model/models/transformed_omie_req.sql
src/engineering_domain_model/models/transformed_omie_wfstatus.sql
src/engineering_domain_model/models/transformed_roc_status_projectname.sql
src/engineering_domain_model/models/transformed_spel_codelists.sql
src/engineering_domain_model/models/transformed_spel_modelitem.sql
src/engineering_domain_model/models/transformed_spel_plantitem.sql
src/engineering_domain_model/models/transformed_spel_t_cable.sql
src/engineering_domain_model/models/transformed_spel_t_equipments.sql
src/engineering_domain_model/models/transformed_spi_component.sql
src/engineering_domain_model/models/transformed_spi_component_function_type.sql
src/engineering_domain_model/models/transformed_spi_component_handle.sql
src/engineering_domain_model/models/transformed_spi_component_location.sql
src/engineering_domain_model/models/transformed_spi_component_sys_io_type.sql
src/engineering_domain_model/models/transformed_spi_drawing.sql
src/engineering_domain_model/models/transformed_spi_line.sql
src/engineering_domain_model/models/transformed_spi_loop.sql
src/engineering_domain_model/models/transformed_spi_plant.sql
src/engineering_domain_model/models/transformed_spi_tag_category.sql
src/engineering_domain_model/models/transformed_spi_udf_component.sql
src/engineering_domain_model/profiles/profiles.yml

#####
## [tag-v0.0.18] - 2025-04-15
### Deployed to:
- **qa**: 2025-04-15
### Changes:
- Deployed version tag-v0.0.18
- Commit Message: r3 models set 2 and qa deployment ### Description of changes ###: R3 changes### Task/Ticket/Issue ###: ADO task r3- set 2 and  qa deployment
- Files Updated: src/engineering_domain_model/models/schema.yml
src/engineering_domain_model/models/transformed_aim_tags_pwrbi.sql
src/engineering_domain_model/models/transformed_mel_cp2_mechanical_eqip.sql
src/engineering_domain_model/models/transformed_spi_component.sql
src/engineering_domain_model/models/transformed_spi_component_function_type.sql
src/engineering_domain_model/models/transformed_spi_component_handle.sql
src/engineering_domain_model/models/transformed_spi_component_location.sql
src/engineering_domain_model/models/transformed_spi_component_sys_io_type.sql
src/engineering_domain_model/models/transformed_spi_udf_component.sql

#####
## [tag-v0.0.19] - 2025-04-15
### Deployed to:
- **qa**: 2025-04-15
### Changes:
- Deployed version tag-v0.0.19
- Commit Message: Feature/e3d v2 fixes ### Description of changes ###: e3d_v2_fixes as raised by Prathamesh### Task/Ticket/Issue ###: ADO task
- Files Updated: src/engineering_domain_model/models/transformed_e3d_gensec_floor_union.sql
src/engineering_domain_model/models/transformed_e3d_pipes.sql

#####
## [tag-v0.0.20] - 2025-04-22
### Deployed to:
- **qa**: 2025-04-22
### Changes:
- Deployed version tag-v0.0.20
- Commit Message: Add files via upload ### Description of changes ###: Equip search key changes### Task/Ticket/Issue ###:  Equip search key changes
- Files Updated: src/engineering_domain_model/models/transformed_e3d_equip.sql

#####
## [tag-v0.0.21] - 2025-04-22
### Deployed to:
- **qa**: 2025-04-22
### Changes:
- Deployed version tag-v0.0.21
- Commit Message: pipes fix and roc status new column addition ### Description of changes ###: e3d_pipes fixes and roc status### Task/Ticket/Issue ###: e3d_pipes fixes and roc status
- Files Updated: src/engineering_domain_model/models/schema.yml
src/engineering_domain_model/models/transformed_e3d_pipes.sql
src/engineering_domain_model/models/transformed_roc_status_projectname.sql

#####
## [tag-v0.0.25] - 2025-04-22
### Deployed to:
- **qa**: 2025-04-22
### Changes:
- Deployed version tag-v0.0.25
- Commit Message: Feature/audit table qa ### Description of changes ###: audit_log_table to qa### Task/Ticket/Issue ###: audit_log_table to qa
- Files Updated: src/engineering_domain_model/dbt_project.yml
src/engineering_domain_model/macros/log_audit_table.sql

#####
## [tag-v0.0.27] - 2025-04-29
### Deployed to:
- **qa**: 2025-04-29
### Changes:
- Deployed version tag-v0.0.27
- Commit Message: Update dbt_engineering_dag.py ### Description of changes ###: dag changes### Task/Ticket/Issue ###: dag changes
- Files Updated: src/dags/dbt_engineering_dag.py

#####
## [tag-v0.0.28] - 2025-04-29
### Deployed to:
- **qa**: 2025-04-29
### Changes:
- Deployed version tag-v0.0.28
- Commit Message: project_code upper case fix ### Description of changes ###: project code  upper case fix### Task/Ticket/Issue ###: project code  upper case fix
- Files Updated: src/engineering_domain_model/models/transformed_ado_modelreviewitemstracker.sql
src/engineering_domain_model/models/transformed_aim_tags_pwrbi.sql
src/engineering_domain_model/models/transformed_e3d_pipes_mto_lists.sql
src/engineering_domain_model/models/transformed_engreg_assumptions_index.sql
src/engineering_domain_model/models/transformed_engreg_hold_index.sql
src/engineering_domain_model/models/transformed_engreg_seal_decision.sql
src/engineering_domain_model/models/transformed_engreg_td_index.sql
src/engineering_domain_model/models/transformed_isotracker_stress_calc.sql
src/engineering_domain_model/models/transformed_isotracker_union.sql
src/engineering_domain_model/models/transformed_mel_cp2_mechanical_eqip.sql
src/engineering_domain_model/models/transformed_mps_logs_all.sql
src/engineering_domain_model/models/transformed_omie_combo.sql
src/engineering_domain_model/models/transformed_omie_wfstatus.sql
src/engineering_domain_model/models/transformed_spi_component_function_type.sql
src/engineering_domain_model/models/transformed_spi_component_handle.sql
src/engineering_domain_model/models/transformed_spi_component_location.sql
src/engineering_domain_model/models/transformed_spi_component_sys_io_type.sql
src/engineering_domain_model/models/transformed_spi_drawing.sql
src/engineering_domain_model/models/transformed_spi_line.sql
src/engineering_domain_model/models/transformed_spi_loop.sql
src/engineering_domain_model/models/transformed_spi_plant.sql
src/engineering_domain_model/models/transformed_spi_tag_category.sql
src/engineering_domain_model/models/transformed_spi_udf_component.sql
