{{
     config(
         materialized = "table",
         tags=["engineering"]
     )

}}

select name_ws,
pidlineno,
clientdocno,
pidref,
unique_line_id,
bore_in,
pspe,
ispe,
tspe,
duty,
rocstatus,
CASE WHEN rocstatus IN ('3-0','4-0') THEN 'ROC0' 
     WHEN rocstatus IN ('3-1','4-1') THEN 'ROC1'   
     WHEN rocstatus IN ('3-2','4-2') THEN 'ROC2' 
     WHEN rocstatus IN ('3-3','4-3') THEN 'ROC3' 
     WHEN rocstatus IN ('3-4','4-4') THEN 'ROC4'  
     WHEN rocstatus IN ('3-5','4-5') THEN 'ROC5' 
     WHEN rocstatus IN ('3-6','4-6') THEN 'ROC6'  
     WHEN rocstatus IN ('3-7','4-7') THEN 'ROC7' 
     WHEN rocstatus IN ('3-8','4-8') THEN 'ROC8'     
ELSE 'No Status' END 
as rocgroup,
roc,
progress,
holdflag,
"zone",
site,
wbs,
wbs_id,
Project_WBS,
cwa,
cwpzone,
cwp1,
search_key1,
project_code AS project_id,
extracted_date,
is_active,
dag_execution_date,
SUM(length1) as length_inch,
case when length_inch = 0 then 0 else length_inch/12 END as thru_length_ft,
SUM(tube_length1) as tube_length_inch,
case when tube_length_inch = 0 then 0 else tube_length_inch/12 END AS tube_length_ft,
max(SPREF1) AS Spec_Size,
coalesce(project_code, '') || '_' || coalesce(wbs, '') || '_' || coalesce(cwa, '')
|| '_' || coalesce(cwpzone, '') as wbskey,
coalesce(project_code, '') || '_' || coalesce(cwp1, '') as cwpkey,
dense_rank() OVER (
                        partition by project_code
                        order by
                            project_code,extracted_date DESC
                    ) as Latest
FROM
(
select 
P.name_ws,
P.pidlineno,
P.clientdocno,
P.pidref,
P.unique_line_id,
P.bore_in,
P.pspe,
P.ispe,
P.tspe,
P.duty,
P.rocstatus,
RIGHT(P.rocstatus,1) as roc,
P.progress,
P.holdflag,
P."zone",
P.site,
P.wbs,
P.cwa,
P.cwpzone,
P.cwp1,
P.search_key1,
P.project_code,
coalesce(P.project_code, '') || '_' || coalesce(P.wbs, '')  as Project_WBS,
case when W.Id is NULL THEN 0 else W.Id END as wbs_id,
P.extracted_date,
case when C."length" = '0.00in' then 0
when C."length" = '' OR C."length" ISNULL then 0
else CAST(REPLACE(C."length",'in','') as numeric(38,4)) END
 as length1,
case when C."length" = '0.00in' and c."type" = 'TUBI' then 0
when C."length" = '' OR C."length" ISNULL  and c."type" = 'TUBI' then 0
when C."type" = 'TUBI' then CAST(REPLACE(C."length",'in','') as numeric(38,4)) 
else 0 END
as tube_length1,
case when C."type" = 'TUBI' then C.SPREF 
else '' End SPREF1,
case when P.extracted_date =
(select max(extracted_date) FROM  {{ source('engineering_dim', 'e3d_pipes') }})
then TRUE
else FALSE
END as is_active,
GETDATE() as dag_execution_date
FROM  {{ source('engineering_dim', 'e3d_pipes') }} P
left OUTER JOIN 
(
    select DISTINCT wbs,project_code, project_code || '_' || wbs  as Project_WBS ,
DENSE_RANK() OVER(order by project_code,wbs) AS Id
FROM  {{ source('engineering_dim', 'e3d_pipes') }} where wbs <> ''
ORDER BY project_code,wbs
) as W
ON Upper(coalesce(P.project_code, '') || '_' || coalesce(P.wbs, '')) = Upper(W.Project_WBS)
left OUTER JOIN  {{ source('engineering_dim', 'e3d_pipes_mto_comps') }} C
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
where P.extracted_date in 
(select top 90 distinct extracted_date from {{ source('engineering_dim', 'e3d_pipes') }} 
order by extracted_date desc)
)
group by 
name_ws,
pidlineno,
clientdocno,
pidref,
unique_line_id,
bore_in,
pspe,
ispe,
tspe,
duty,
rocstatus,
roc,
progress,
holdflag,
"zone",
site,
wbs,
cwa,
cwpzone,
cwp1,
search_key1,
project_code,
Project_WBS,
wbs_id,
extracted_date,
is_active,
dag_execution_date