{{
     config(
         materialized = "table",
         tags=["engineering"]
     )

}}

select 
p.rocstatus,
CASE WHEN p.rocstatus IN ('3-0','4-0') THEN 'ROC0' 
     WHEN p.rocstatus IN ('3-1','4-1') THEN 'ROC1'   
     WHEN p.rocstatus IN ('3-2','4-2') THEN 'ROC2' 
     WHEN p.rocstatus IN ('3-3','4-3') THEN 'ROC3' 
     WHEN p.rocstatus IN ('3-4','4-4') THEN 'ROC4'  
     WHEN p.rocstatus IN ('3-5','4-5') THEN 'ROC5' 
     WHEN p.rocstatus IN ('3-6','4-6') THEN 'ROC6'  
     WHEN p.rocstatus IN ('3-7','4-7') THEN 'ROC7' 
     WHEN p.rocstatus IN ('3-8','4-8') THEN 'ROC8'     
ELSE 'No Status' END 
as rocgroup,
P.progress,
l.line_num,
CASE WHEN line_num <> '' then 'In-Line' else 'Off-line' End as Inst_type,
CASE WHEN Inst_type = 'In-Line' then 1 else 0 End as In_Line,
C.tag_name,
C.pipe as line_no,
C.iso as iso_no,
C.ewp,
C.p_id_line_no,
C.type as Comp_type,
C.description AS Instrument_Type,
cast(C.project_code as varchar(100)) AS project_id,
coalesce(C.wbs1, '') as wbs,
coalesce(C.cwa1, '') as cwa,
coalesce(C.cwpzone1, '') as cwpzone,
coalesce(C.cwp1, '') as cwp,
C.project_code_wbs_cwa_cwpzone as wbs_key,
C.search_key,
C.extracted_date,
GETDATE() as dag_execution_date
from
     {{ source('engineering_dim', 'transformed_e3d_pipes_mto_comps') }} as C
     LEFT OUTER JOIN {{ source('engineering_dim', 'transformed_e3d_pipes') }} p
     ON Upper(C.pipe_ws) || '_' || C.extracted_date = Upper(P.name_ws) || '_' || P.extracted_date 
     left join {{ source('engineering_dim', 'transformed_spi_line') }} l
     on Upper(C.project_code) || '_' || Upper(C.p_id_line_no) || '_' || C.extracted_date =
     Upper(l.project_code) || '_' || Upper(l.line_num) || '_' || l.extracted_date
     where C.type = 'INST' and In_Line = 1
