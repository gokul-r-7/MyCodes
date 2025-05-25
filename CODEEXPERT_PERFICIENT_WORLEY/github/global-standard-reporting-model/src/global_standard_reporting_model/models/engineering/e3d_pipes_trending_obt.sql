{{
     config(
         materialized = "table",
         tags=["engineering"]
     )

}}

select *, 
CASE when Progress = 0 then 0 
     when Line_count = 0 then 0
     else  Progress/Line_count 
     END as Line_progress_perc,
Case when progress = 0 then 0
     when iso_count= 0 then 0
     else progress/iso_count
     END as iso_progress_perc,
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
as rocgroup
from
(select 
project_code AS project_id,
wbs,
cwa,
cwpzone,
cwp1,
rocstatus,
extracted_date,
coalesce(project_code, '') || '_' || coalesce(wbs, '') || '_' || coalesce(cwa, '')
|| '_' || coalesce(cwpzone, '') as wbskey,
coalesce(project_code, '') || '_' || coalesce(wbs, '')  as Project_WBS,
sum(progress) as Progress,
count (DISTINCT unique_line_id) as dist_Line_count,
count (DISTINCT name) as dist_Line_segment_count,
count (DISTINCT clientdocno) as dist_iso_count,
count (unique_line_id) as Line_count,
count (name) as Line_segment_count,
count (clientdocno) as iso_count,
GETDATE() as dag_execution_date
from  {{ source('engineering_dim', 'e3d_pipes') }}
group by project_code, wbs, cwa, cwpzone, cwp1,rocstatus, extracted_date )
