{{
     config(
         materialized = "table",
         tags=["supply_chain"]
     )

}}

SELECT DISTINCT
    proj_id as project_id,
    tmr_number,
    sub_project,
    discipline_id,
    discipline,
    title,
    tmr_originator,
    ros_date,
    internal_status,
    issue_purpose,
    origin,
    etl_load_date as dataset_refershed_date
FROM
    {{ source('supply_chain_dim', 'erm_tmr_data') }}
