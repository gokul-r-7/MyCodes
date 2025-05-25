{{
     config(
         materialized = "table",
         tags=["engineering"]
     )

}}

select * from
(select C.CMPNT_NAME,
C.CMPNT_FUNC_TYPE_ID,
F.phy_instru,
C.CMPNT_HANDLE_ID,
H.CMPNT_HANDLE_NAME,
H.CMPNT_HANDLE_DESC,
CASE WHEN CMPNT_HANDLE_DESC = 'DELETE - NO LONGER REQUIRED' THEN 1 ELSE 0 END AS Delete_flag,
case when POSITION('NEW' in CMPNT_HANDLE_NAME) > 0 OR CMPNT_HANDLE_NAME = 'BY MECH' then 1 else 0 end new_or_by_mech,
case when new_or_by_mech = 1 AND phy_instru = 'y' 
AND Delete_flag = 0 then 1 else 0 END Worley_Direct_Procured,
case when CMPNT_HANDLE_NAME = 'BY VENDOR' AND phy_instru = 'y' 
AND Delete_flag = 0 then 1 else 0 END Furnished_with_Mech_Equipment,
case when POSITION('OFE' in CMPNT_HANDLE_NAME) > 0  AND phy_instru = 'y' 
AND Delete_flag = 0 then 1 else 0 END OFE,
case when Worley_Direct_Procured = 0  AND Furnished_with_Mech_Equipment = 0 AND OFE = 0
AND CMPNT_HANDLE_NAME = 'BY VENDOR' AND phy_instru = 'y' AND Delete_flag = 0 then 1 else 0 END undefined,
C.cmpnt_sys_io_type_id,
I.cmpnt_sys_io_type_name,
CASE WHEN phy_instru = 'y' AND Delete_flag = 0 then 1 else 0 END all_instrument,
LEFT(cmpnt_sys_io_type_name,1) AS PRE_CMPNT_SYS_IO_TYPE_NAMe,
case when PRE_CMPNT_SYS_IO_TYPE_NAMe <> 'S' and PRE_CMPNT_SYS_IO_TYPE_NAMe <> '' and
PRE_CMPNT_SYS_IO_TYPE_NAMe <> '-' AND phy_instru = 'y' AND Delete_flag = 0 then 1 else 0 END Hardwired_IOs,
F.CMPNT_FUNC_TYPE_DESC AS Instrument_Type,
L.cmpnt_loc_name as Component_location_name,
cast(C.project_code as varchar(100)) AS project_id,
coalesce(C.wbs, '') as wbs,
coalesce(C.cwa, '') as cwa,
coalesce(C.cwpzone, '') as cwpzone,
C.project_code_wbs_cwa_cwpzone as wbs_key,
C.extracted_date,
GETDATE() as dag_execution_date
,dense_rank() OVER (
                    partition by C.project_code
                    order by
                    C.extracted_date  DESC
                                ) as rn
from {{ source('engineering_dim', 'transformed_spi_component') }} C
left join {{ source('engineering_dim', 'transformed_spi_component_function_type') }} F
on C.CMPNT_FUNC_TYPE_ID || '_' || C.project_code || '_' || C.extracted_date   = F.CMPNT_FUNC_TYPE_ID || '_' || F.project_code || '_' || F.extracted_date
LEFT JOIN {{ source('engineering_dim', 'transformed_spi_component_handle') }} H 
on C.CMPNT_HANDLE_ID || '_' || C.project_code || '_' || C.extracted_date = H.CMPNT_HANDLE_ID || '_' || H.project_code || '_' || H.extracted_date
LEFT JOIN {{ source('engineering_dim', 'transformed_spi_component_sys_io_type') }} I 
on C.cmpnt_sys_io_type_id || '_' || C.project_code || '_' || C.extracted_date = I.cmpnt_sys_io_type_id || '_' || I.project_code || '_' || I.extracted_date
LEFT JOIN {{ source('engineering_dim', 'transformed_spi_component_location') }} L 
on C.cmpnt_loc_id || '_' || C.project_code || '_' || C.extracted_date = L.cmpnt_loc_id || '_' || L.project_code || '_' || L.extracted_date
) where  rn = 1