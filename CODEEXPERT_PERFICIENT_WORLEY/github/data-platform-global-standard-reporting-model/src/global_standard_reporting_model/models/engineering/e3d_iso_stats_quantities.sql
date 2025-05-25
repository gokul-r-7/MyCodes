{{
     config(
         materialized = "table",
         tags=["engineering"]
     )

}}

select * from
(
SELECT
     C.pipe_ws,
     C.iso,
     C.type,
     C.tag_name,
     C.material,
     C.description,
     C.spref as Piping_Specs,
     C.Piping_Specification,
     C.ispec,
     C.nb1,
     C."length",
     cast(REPLACE(C.nb1, 'in', '') as  decimal(38, 2)) as Pipe_Diameter,
     cast(REPLACE(C."length", 'in', '')  as decimal(38, 2)) as Thru_Length,
     cast(case when Thru_Length = 0 then 0 
     ELSE (Thru_Length/12) END as decimal(38, 2)) as Thru_Length_ft,
     cast(case when Thru_Length = 0 then 0 
     ELSE (Thru_Length/3.28) END as decimal(38, 2)) as Length_ft,
     coalesce(Pipe_Diameter * Thru_Length, 0) as avg_diameter,
     CAST(case when Pipe_Diameter = 0 then 0
          when Thru_Length = 0 then 0 ELSE
          (Pipe_Diameter/Thru_Length) END as decimal(38, 2)) as avg_diameter1,     
     case
     when POSITION('CS' in C.material) > 0 then 'Carbon Steel'
     when POSITION('SS' in C.material) > 0 then 'Stainless Steel'
     when POSITION('CR' in C.material) > 0 then 'Alloy'
     ELSE 'Other' END as Materials,
     CASE
     WHEN Pipe_Diameter <= 4 THEN '4" & smaller'
     WHEN Pipe_Diameter > 4
     AND  Pipe_Diameter <= 12 THEN 'Between 4" and 12"'
     WHEN Pipe_Diameter > 12 THEN 'Larger than 12"'
     ELSE 'Size N/A' END as Pipe_Size,
    CASE
     WHEN Pipe_Diameter <= 4 THEN '4" & smaller'
     WHEN Pipe_Diameter > 4  THEN 'Larger than 4"'
     ELSE 'Size N/A' END as Pipe_Size1,
     C.wgt,
     cast(REPLACE(C.wgt, 'kg', '') as decimal(38, 4)) as Weight_kg,
     cast(CASE WHEN Weight_kg = 0 THEN 0
          ELSE (Weight_kg*2.205) END  as decimal(38, 4)) AS Weight_lbs,
     case
     when C.type = 'WELD' AND C.description = 'SHOP WELD'
     AND (LEFT(C.tag_name, 3)) <> '/CW' THEN 'Shop Weld'
     when C.type = 'WELD' AND C.description = 'FIELD FIT WELD'
     AND (LEFT(C.tag_name, 3)) <> '/CW' THEN 'Field Fit Weld'
     when C.type = 'WELD' AND C.description = 'FIELD WELD'
     AND (LEFT(C.tag_name, 3)) <> '/CW' THEN 'Field Weld'
     when C.type = 'WELD' AND (LEFT(C.tag_name, 3)) = '/CW' THEN 'Closure Weld' 
     ELSE '' END as closure_Weld_Description,
     case
     when C.type not in ('ATTA', 'TUBI', 'VALV', 'WELD', 'INST', 'GASK') then 'Flanges'
     else '' END as Flanges,
     case
     when C.type not in ('ATTA', 'TUBI', 'VALV', 'WELD', 'INST', 'GASK') then 1
     else 0 END as fit_and_Flanges,
     Case when C.type = 'TUBI' then 1 else 0 end as Tubi, 
     Case when C.type = 'VALV' then 1 else 0 end as Valve, 
     case
     when POSITION('RF' in C.description) > 0 AND C.type = 'VALV' then 'Flanged Valve'
     when POSITION('FF' in C.description) > 0 AND C.type = 'VALV' then 'Flanged Valve'
     when POSITION('SW' in C.description) > 0 AND C.type = 'VALV' then 'Welded Valve'
     when POSITION('BE' in C.description) > 0 AND C.type = 'VALV' then 'Welded Valve'
     when C.type = 'VALV' then 'Valve'
     ELSE '' END as Valve_type,
     case when Valve_type in ('Flanged Valve') then 1 else 0 end Flanged_Valve,
     case when Valve_type in ('Welded Valve') then 1 else 0 end Welded_Valve,     
     C.extracted_date,
     C.search_key,
     case when C1.Materials = 'Alloy' then 'Alloy'
          when C1.Materials = 'Carbon Steel' then 'Carbon Steel'
          when C1.Materials = 'Stainless Steel' then 'Stainless Steel'
          else 'Other' End as weld_material,
     P.TSPE,
     cast(C.project_code as varchar(100)) AS project_id,
     coalesce(C.wbs1, '') as wbs,
     coalesce(C.cwa1, '') as cwa,
     coalesce(C.cwpzone1, '') as cwpzone,
     coalesce(C.cwp1, '') as cwp,
     coalesce(C.project_code, '') || '_' || coalesce(C.wbs1, '') || '_' || coalesce(C.cwa1, '')
     || '_' || coalesce(C.cwpzone1, '') as wbskey,
     coalesce(C.project_code, '') || '_' || coalesce(C.wbs1, '')  as Project_WBS,
     coalesce(C.project_code, '') || '_' || coalesce(C.cwp1, '')  as cwpkey,
     GETDATE() as dag_execution_date,
     dense_rank() OVER (
                        partition by C.project_code
                        order by
                            C.project_code,C.extracted_date DESC
                    ) as rn     
from
     {{ source('engineering_dim', 'transformed_e3d_pipes_mto_comps') }} as C
     left join (
select * from (
               select
               DISTINCT pipe_ws,           
               type,
               project_code,   
               extracted_date,
               case
               when POSITION('CS' in material) > 0 then 'Carbon Steel'
               when POSITION('SS' in material) > 0 then 'Stainless Steel'
               when POSITION('CR' in material) > 0 then 'Alloy'
               ELSE 'Other' END as Materials,
               dense_rank() OVER (
                        partition by project_code,pipe_ws
                        order by Materials ASC
                    ) as rn1
from
     {{ source('engineering_dim', 'transformed_e3d_pipes_mto_comps') }}  where type = 'TUBI' )
     where rn1 = 1     
                ) as C1 
    on Upper(C.project_code) || '_' || Upper(C.pipe_ws) || '_' || C.extracted_date  = Upper(C1.project_code) || '_' || Upper(C1.pipe_ws) || '_' || C1.extracted_date
    LEFT OUTER JOIN {{ source('engineering_dim', 'transformed_e3d_pipes') }} p
    ON Upper(C.pipe_ws) || '_' || C.extracted_date = Upper(P.name_ws) || '_' || P.extracted_date 
)
     where
            rn = 1