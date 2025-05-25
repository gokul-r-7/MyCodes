{{
     config(
         materialized = "table",
         tags=["engineering"]
     )
}}

select 
P.workpack_no,
P.suppo,
P.support_type,
P.bore_inch,
P.tag_name,
P.quantity,
P.pid_line_no,
P.isometric,
P.project_code AS project_id,
case 
when P.bore_inch = 'nan' then '0'
when P.bore_inch = '' then '0'
when LENGTH(P.bore_inch) > 6 then '0' else P.bore_inch end as Pipe_Size,
CAST(Pipe_Size as NUMERIC(38,2)) as Pipe_Size_in,
case when P.support_type = '' then '' 
else (LEFT(TRIM(P.support_type),2)) END as support_type2,
C.Materials AS Pipe_Material_Type,
Split_part(coalesce(P.site, ''), '-', 2) AS wbs,
case when P.workpack_no = '' then '' else Substring(P.workpack_no,2,2) END AS cwa,
case when P.workpack_no = '' then '' else Substring(P.workpack_no,2,4) END AS cwpzone,
coalesce(P.project_code, '') || '_' || coalesce(wbs, '') || '_' || coalesce(cwa, '')
|| '_' || coalesce(cwpzone, '') as wbskey,
coalesce(P.project_code, '') || '_' || coalesce(wbs, '')  as Project_WBS,
coalesce(P.project_code, '') || '_' || coalesce(P.workpack_no, '')  as CWP_key,
'Total' as Support_TypeV,
P.extracted_date,
GETDATE() as dag_execution_date
FROM  {{ source('engineering_dim', 'e3d_pipe_support_attr') }} as P
left join (
        with MaterialsCTE AS(
            select
               DISTINCT pipe_ws,
               type,
               case
               when POSITION('CS' in material) > 0 then 'Carbon Steel'
               when POSITION('SS' in material) > 0 then 'Stainless Steel'
               when POSITION('CR' in material) > 0 then 'Alloy'
               ELSE 'Other' END as Materials,
               row_number() OVER (
                                    partition by pipe_ws                                    
                                    order by
                                        pipe_ws DESC
                                ) as rn
          from
               {{ source('engineering_dim', 'e3d_pipes_mto_comps') }}
          where
               extracted_date = (select max(extracted_date) FROM {{ source('engineering_dim', 'e3d_pipes_mto_comps') }} )
               and type = 'TUBI'
        )
         SELECT
                            pipe_ws,
                            Materials
                        from
                            MaterialsCTE
                        where
                            rn <= 1
            ) as C 
on P.isometric = C.pipe_ws
where 
P.extracted_date = (select max(extracted_date) FROM {{ source('engineering_dim', 'e3d_pipe_support_attr') }})