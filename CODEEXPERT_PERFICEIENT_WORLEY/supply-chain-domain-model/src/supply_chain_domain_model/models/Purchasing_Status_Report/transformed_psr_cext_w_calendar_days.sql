{%- set run_date = "CURRENT_TIMESTAMP()" -%}


{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='false', 
        custom_location=target.location ~ 'transformed_psr_cext_w_calendar_days/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}


select 
    ma.proj_no,
    ma.proc_sch_hdr_no,
    ma.proc_sch_item_no,
    ma.proc_milestone_no,
    case
        when ma.calendar_no is null
        then coalesce(
            (select max(p.default_calendar_no)  -- <-- Using MAX() ensures only one row
            from {{ source('curated_erm', 'proj') }} p
            where p.proj_no = ma.proj_no
            and p.is_current = 1),
            0
        )
        else ma.calendar_no
    end as calendar_no,
    coalesce(cast(ma.id as string), '') as id,
    case 
        when ma.no_of_days_working != 0 then cast(ma.no_of_days_working as float)
        else (
        select 
            cast(sum(case when cl.day1_workingdaymodel_no is null then 0 else 1 end) as float) +
            cast(sum(case when cl.day2_workingdaymodel_no is null then 0 else 1 end) as float) +
            cast(sum(case when cl.day3_workingdaymodel_no is null then 0 else 1 end) as float) +
            cast(sum(case when cl.day4_workingdaymodel_no is null then 0 else 1 end) as float) +
            cast(sum(case when cl.day5_workingdaymodel_no is null then 0 else 1 end) as float) +
            cast(sum(case when cl.day6_workingdaymodel_no is null then 0 else 1 end) as float) +
            cast(sum(case when cl.day7_workingdaymodel_no is null then 0 else 1 end) as float)
            from {{ source('curated_erm', 'calendar') }} cl
            inner join {{ source('curated_erm', 'proj') }} pj 
            on cl.calendar_no = pj.default_calendar_no
            where pj.proj_no = ma.proj_no and cl.is_current=1
            )
    end as no_of_working_days,
    ma.etl_load_date,
    {{run_date}} as model_created_date,
    {{run_date}} as model_updated_date,
    {{ generate_load_id(model) }} as model_load_id
from 
(select  
    psi.proj_no,     
    psi.proc_sch_hdr_no,
    psi.proc_sch_item_no,
    psi.proc_milestone_no,
    coalesce(cast(cal.calendar_no as float), 0) as calendar_no,
    cal.id, 
    case when day1_workingdaymodel_no is null then 0 else 1 end +
    case when day2_workingdaymodel_no is null then 0 else 1 end +
    case when day3_workingdaymodel_no is null then 0 else 1 end +
    case when day4_workingdaymodel_no is null then 0 else 1 end +
    case when day5_workingdaymodel_no is null then 0 else 1 end +
    case when day6_workingdaymodel_no is null then 0 else 1 end +
    case when day7_workingdaymodel_no is null then 0 else 1 end as no_of_days_working,
    cast(psi.execution_date as date) as etl_load_date
from 
        {{ source('curated_erm', 'proc_sch_item') }} psi
left join 
 {{ source('curated_erm', 'calendar') }} cal
    on psi.calendar_no = cal.calendar_no
    and cal.is_current = 1
left join 
    {{ source('curated_erm', 'proj') }} proj
    on psi.calendar_no = proj.default_calendar_no
    and psi.proj_no = proj.proj_no
    and proj.is_current = 1 
where  
     psi.is_current = 1  ) ma
     