{{
     config(
         materialized = "table",
         tags=["engineering"]
     )

}}

select 
g.STRU as Owner,
g.NAME as stru_name,
g.StlWeightClass,
g.SPRE,
g.MATR,
g.FIREPROOFING,
g.reference_of_the_element,
g.STRUCL as class,
g.MTO_TYPE1,
case when g.mto_type1 in 
('FUTR',
'MODULES',
'MPS_MES',
'PLTFMS',
'STEEL',
'STK_BLT') then 'Steel' 
when g.mto_type1 = 'FDNS' then 'Foundations' 
when g.mto_type1 in ('PILING','PILING_TYPE_1') then 'Piling' END as material_type,
case when material_type = 'Steel' then 1
     when material_type = 'Foundations' then 2
     when material_type = 'Piling' then 3 else 0 end material_type_code,
cast(case when g.nett_weight = 0  then 0.00 
          when g.nett_weight = '' then 0.00 ELSE cast(g.nett_weight as decimal(38,4)) end as decimal(38,4)) nett_weight_kg,
cast(case when g.nett_weight = 0  then 0 
          when g.nett_weight = '' then 0 ELSE 
          (nett_weight_kg/907.2) end as decimal(38, 4)) as nett_weight_us_ton,
cast(case when g.cut_length_in = 0  then 0.00 
          when g.cut_length_in = '' then 0.00 ELSE cast(g.cut_length_in as decimal(38,4)) end as decimal(38,4)) cut_length_in1,
cast(case when g.cut_length_in = 0  then 0 
          when g.cut_length_in = '' then 0 ELSE 
          (cut_length_in1/12) end as decimal(38, 4)) as cut_length_ft,
cast(case when g.nett_volume_in3 = 0  then 0.00 
          when g.nett_volume_in3 = '' then 0.00 ELSE cast(g.nett_volume_in3 as decimal(38,4)) end as decimal(38,4)) nett_volume_in3_1,
cast(case when g.nett_volume_in3 = 0  then 0 
          when g.nett_volume_in3 = '' then 0 ELSE 
          (nett_volume_in3_1/46656) end as decimal(38, 4)) as nett_volume_CuYd,
g.stvval,
s.rocstatus,
CASE WHEN s.rocstatus IN ('3-0','4-0') THEN 'ROC0' 
     WHEN s.rocstatus IN ('3-1','4-1') THEN 'ROC1'   
     WHEN s.rocstatus IN ('3-2','4-2') THEN 'ROC2' 
     WHEN s.rocstatus IN ('3-3','4-3') THEN 'ROC3' 
     WHEN s.rocstatus IN ('3-4','4-4') THEN 'ROC4'  
     WHEN s.rocstatus IN ('3-5','4-5') THEN 'ROC5' 
     WHEN s.rocstatus IN ('3-6','4-6') THEN 'ROC6'  
     WHEN s.rocstatus IN ('3-7','4-7') THEN 'ROC7' 
     WHEN s.rocstatus IN ('3-8','4-8') THEN 'ROC8'     
ELSE 'No Status' END 
as rocgroup,
s.progress,
s.WORKPACKNO,
g.site,
g.zone,
g.wbs,
g.cwa,
g.cwpzone,
g.cwp1,
g.project_code_wbs_cwa_cwpzone as wbs_key,
cast(g.project_code as varchar(100)) AS project_id,
g.extracted_date,
case when g.extracted_date =
(select max(extracted_date) FROM {{ source('engineering_dim', 'transformed_e3d_gensec_floor_union') }})
then TRUE
else FALSE
END as is_active,
GETDATE() as dag_execution_date
from {{ source('engineering_dim', 'transformed_e3d_gensec_floor_union') }} g
left join {{ source('engineering_dim', 'transformed_e3d_struc') }} s
on g.project_code  || '_' || g.STRU  || '_' || g.extracted_date =
 s.project_code  || '_' || s.name  || '_' || s.extracted_date
 where g.extracted_date in 
(select top 30 distinct extracted_date from {{ source('engineering_dim', 'transformed_e3d_gensec_floor_union') }}
order by extracted_date desc) and material_type_code <>0