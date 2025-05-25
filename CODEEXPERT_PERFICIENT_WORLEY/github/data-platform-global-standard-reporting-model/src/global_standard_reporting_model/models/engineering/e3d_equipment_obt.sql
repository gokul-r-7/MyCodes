{{
     config(
         materialized = "table",
         tags=["engineering"]
     )

}}

select * from
(
select 
e.Name as eq_name,
e.me_func_code,
m.description,
CASE when m.description = '' then 0 
     when m.description is null then 0 else 1 end me_func_code_flag,
e.ROCstatus,
CASE WHEN e.rocstatus IN ('3-0','4-0') THEN 'ROC0' 
     WHEN e.rocstatus IN ('3-1','4-1') THEN 'ROC1'   
     WHEN e.rocstatus IN ('3-2','4-2') THEN 'ROC2' 
     WHEN e.rocstatus IN ('3-3','4-3') THEN 'ROC3' 
     WHEN e.rocstatus IN ('3-4','4-4') THEN 'ROC4'  
     WHEN e.rocstatus IN ('3-5','4-5') THEN 'ROC5' 
     WHEN e.rocstatus IN ('3-6','4-6') THEN 'ROC6'  
     WHEN e.rocstatus IN ('3-7','4-7') THEN 'ROC7' 
     WHEN e.rocstatus IN ('3-8','4-8') THEN 'ROC8'     
ELSE 'No Status' END 
as rocgroup,
e.progress,
e.discipline,
e.wbs,
e.cwa,
e.cwpzone,
e.cwp1 as cwp,
cast(e.project_code as varchar(100)) AS project_id,
e.project_code_wbs_cwa_cwpzone as wbskey,
e.search_key,
e.extracted_date,
GETDATE() as dag_execution_date,
dense_rank() OVER (
                    partition by e.project_code
                    order by
                    e.extracted_date  DESC
                                ) as rn
FROM {{ source('engineering_dim', 'transformed_e3d_equip') }} e 
left outer join {{ source('engineering_dim', 'transformed_me_function_code') }} m 
on e.me_func_code = m.designator 
)
where rn = 1 and me_func_code_flag = 1