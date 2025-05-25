{{
     config(
         materialized = "table",
         tags=["engineering"]
     )

}}

select * from 
(select * ,dense_rank() OVER (
                                    partition by project_id
                                    order by
                                        project_id,extracted_date  DESC
                                ) as rn
          from {{ ref('e3d_pipe_obt') }} ) T1
where rn = 1