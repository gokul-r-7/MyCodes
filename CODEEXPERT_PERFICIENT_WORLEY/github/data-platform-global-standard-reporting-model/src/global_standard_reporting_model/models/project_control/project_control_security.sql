{{
    config(
        materialized = 'incremental',
        incremental_strategy='append',
        unique_key= ['project_id','user_name'],
        on_schema_change='fail',
        tags=["project_control"]

    )
}}

-- with empty_table as (
--     select
--     ''::varchar(100) as project_id,
--     ''::varchar(250) as user_name
-- )

-- select * from empty_table
-- -- This is a filter so we will never actually insert these values
-- where 1 = 0

with userssecorg as
(
    select
    --Users having Enterprise Level Access
        user_id,org_id
    from {{ source('Project_control_domain_integrated_model', 'transformed_ecosys_org_access') }}
    where org_id = 1

    union

    select
    --Users having Cost Object Super user access
        user_id,'1' as org_id
    from {{ source('Project_control_domain_integrated_model', 'transformed_ecosys_users') }}
    where costobjectsuperuser = 'true'

    union

    select
    --Users having only Sec Org access
        user_id,org_id
    from {{ source('Project_control_domain_integrated_model', 'transformed_ecosys_org_access') }}
    where org_id <> 1
    and user_id not in (
                        select user_id
                        from {{ source('Project_control_domain_integrated_model', 'transformed_ecosys_users') }} where costobjectsuperuser = 'true'
                    )
),
flattenedorgstructure as
(
    select
        path,
        hq_id,
        region_id,
        sub_region_id,
        location_l1_id,
        location_l2_id,
        location_l3_id,
        location_l4_id,
        location_l5_id,
        location_l6_id
    from {{ source('Project_control_domain_integrated_model', 'transformed_ecosys_org_hierarchy') }}
),
usersecorgaccess as
(
    select 
        sc.user_id,
        sc.org_id,
        coalesce(
            nullif(location_l6_id, ''),nullif(location_l5_id, ''),nullif(location_l4_id, ''),
            nullif(location_l3_id, ''),nullif(location_l2_id, ''),nullif(location_l1_id, ''),nullif(sub_region_id, ''),nullif(region_id, ''),hq_id
        ) as secorg_split
    from userssecorg sc
    join flattenedorgstructure fs on
                                (
									sc.org_id = fs.hq_id OR
                                    sc.org_id = fs.region_id OR
                                    sc.org_id = fs.sub_region_id OR
                                    sc.org_id = fs.location_l1_id OR 
                                    sc.org_id = fs.location_l2_id OR
                                    sc.org_id = fs.location_l3_id OR
                                    sc.org_id = fs.location_l4_id OR
                                    sc.org_id = fs.location_l5_id OR
                                    sc.org_id = fs.location_l6_id
								)
),
projectlist as 
(
    select 
        project_no,
        ownerorganizationid as org_id
    from {{ source('Project_control_domain_integrated_model', 'transformed_ecosys_header') }}
)
select
    user_id as user_name,
    project_no as project_id
from projectlist pl
join usersecorgaccess ua on pl.org_id = ua.secorg_split

union 

select
    'AAD:' || user_id as user_name,
    project_id
from {{ source('Project_control_domain_integrated_model', 'transformed_ecosys_project_access') }}