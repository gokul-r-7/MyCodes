{{
     config(
         materialized = "table",
         tags=["engineering"]
     )

}}

select 
DISTINCT
cast(P.project_code as varchar(100)) AS project_id,
P.name_ws,
P.extracted_date,
case when C."type" = 'TUBI' then C.SPREF 
else '' End SPREF1,
case when P.extracted_date =
(select max(extracted_date) FROM  {{ source('engineering_dim', 'transformed_e3d_pipes') }})
then TRUE
else FALSE
END as is_active,
GETDATE() as dag_execution_date
FROM  {{ source('engineering_dim', 'transformed_e3d_pipes') }} P
left OUTER JOIN  {{ source('engineering_dim', 'transformed_e3d_pipes_mto_comps') }} C
    ON 
    Upper((
    CASE
    WHEN P.NAME LIKE '/%' THEN
    Substring(P.NAME, 2,
    Length(P.NAME) - 1)
    ELSE P.NAME
    END ))
    || '_'
    || P.extracted_date
    = CASE
    WHEN Substring(Upper(C.pipe), 1, 1) = '/' THEN
    Concat('', Substring(Upper(C.pipe), 2
    ))
    ELSE Upper(C.pipe)
    END
    || '_'
    || C.extracted_date 
where P.extracted_date =
(select max(extracted_date) FROM  {{ source('engineering_dim', 'transformed_e3d_pipes') }}) AND C."type" = 'TUBI'
