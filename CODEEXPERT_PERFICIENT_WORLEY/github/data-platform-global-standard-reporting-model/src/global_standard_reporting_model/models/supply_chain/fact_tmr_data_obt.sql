{{
     config(
         materialized = "table",
         tags=["supply_chain"]
     )

}}

SELECT DISTINCT
    cast(proj_id as varchar(100)) as project_id,
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
    etl_load_date as refreshed_date
FROM
    {{ source('supply_chain_dim', 'transformed_erm_tmr_data') }}