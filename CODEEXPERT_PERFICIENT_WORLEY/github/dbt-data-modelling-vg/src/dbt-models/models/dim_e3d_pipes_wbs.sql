{{
    config(
        materialized='table'
    )
}}

select extracted_date
      ,wbs
      ,row_number() over (partition by extracted_date order by wbs) id FROM
 (
   select distinct t.execution_date extracted_date
                 , right(site,3) wbs
            from dev.vg_e3d.curated_vg_e3d_vglpipes_sheet t
            inner join (
                        select execution_date
                              , max(TO_DATE(execution_date,'YYYY-mm-dd')) as MaxDate
                          from dev.vg_e3d.curated_vg_e3d_vglpipes_sheet
                          group by execution_date
                       ) t1 on t.execution_date = t1.execution_date and t.execution_date = t1.MaxDate
            where right(site,3) is not null
 )

 --              {% if is_incremental() %}
 --                 where execution_date >= (select coalesce(max(execution_date),'1900-01-01') from {{ this }} )
 --              {% endif %}