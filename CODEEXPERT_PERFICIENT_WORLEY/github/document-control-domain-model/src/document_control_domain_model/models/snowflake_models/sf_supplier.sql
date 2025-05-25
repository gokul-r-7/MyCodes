{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = "CURRENT_DATE" -%}

{{
    config(
        materialized = "table",
        tags=["snowflake_models"]
    )
}}


select 
  file, 
  package_number, 
  document_no as document_number, 
  s.title, 
  revision, 
  submission_status, 
  S.review_status, 
  assigned_to_org, 
  to_date(
    case when due_in is null 
    or due_in = '' then null else due_in end, 
    'dd/mm/yyyy'
  ) as due_in, 
  to_date(
    case when due_out is null 
    or due_out = '' then null else due_out end, 
    'dd/mm/yyyy'
  ) as due_out, 
  to_date(
    case when S.milestone_date is null 
    or S.milestone_date = '' then null else S.milestone_date end, 
    'dd/mm/yyyy'
  ) as milestone_date, 
  lock, 
  to_date(
    case when actual_in is null 
    or actual_in = '' then null else actual_in end, 
    'dd/mm/yyyy'
  ) as actual_in, 
  to_date(
    case when actual_out is null 
    or actual_out = '' then null else actual_out end, 
    'dd/mm/yyyy'
  ) as actual_out, 
  as_built_required, 
  assigned_to_users as assigned_to, 
  attribute_1, 
  attribute_2 as tag_number, 
  attribute_3, 
  attribute_4 as sdr_codes, 
  S.category as document_identifier, 
  check_1 as design_critical, 
  comments, 
  comments_in, 
  comments_out, 
  confidential, 
  contractor_doc_no, 
  contractor_rev, 
  created_by, 
  date_2, 
  date_created, 
  date_to_client, 
  date_modified, 
  days_late, 
  description, 
  discipline as area_or_asset_number, 
  S.file_name, 
  to_date(
    case when S.planned_submission_date is null 
    or S.planned_submission_date = '' then null else S.planned_submission_date end, 
    'dd/mm/yyyy'
  ) as planned_submission_date, 
  print_size, 
  project_field_3, 
  reference, 
  required_by, 
  S.review_source, 
  select_list_1, 
  select_list_2 as FILE_CLASSIFICATION, 
  select_list_3, 
  select_list_4, 
  select_list_5, 
  select_list_6, 
  select_list_7, 
  select_list_8 as purchase_order_number, 
  select_list_9, 
  select_list_10, 
  "size", 
  status, 
  COALESCE(submission_sequence, 0) as submission_sequence,
  supplied_by, 
  tag_no, 
  transmittal, 
  type as DOCUMENT_TYPE_NAME, 
  vdr_code as discipline, 
  vendor_doc_no, 
  vendor_rev, 
  "version", 
  project_id, 
  Project_code, 
  instance_name, 
  to_date(
    case when execution_date is null 
    or execution_date = '' then null else execution_date end, 
    'yyyy-mm-dd'
  ) as execution_date, 
  S.source_system_name, 
  p.projectname as aconex_project_name,
  --R.file_type as FILE_CLASSIFICATION, 
  -- T.Tag_number, 
  -- SD.sdr_code as sdr_codes, 
  --M.asset_code || '-' || M.asset_name as area_or_asset_number, 
  --M.type_code || ' - ' || M.type_name as document_identifier, 
  {{run_date}} as created_date,
  {{run_date}} as updated_date,
  {{ generate_load_id(model) }} as load_id 
from 
  {{ source('curated_aconex', 'curated_supplier_report') }}   S 
  left join {{ ref('sf_dim_project') }} p on p.project = S.project_id 
  -- left join {{ ref('sf_master') }} M on M.document_number = S.document_no and M.meta_project_rls_key = S.project_id 
  --left join {{ ref('sf_revision') }} R on R.document_sid = M.document_sid 
  -- left join {{ ref('sf_tag_to_document') }} T on T.document_sid = M.document_sid 
  -- left join {{ ref('sf_sdr_to_document') }} SD on SD.document_sid = M.document_sid