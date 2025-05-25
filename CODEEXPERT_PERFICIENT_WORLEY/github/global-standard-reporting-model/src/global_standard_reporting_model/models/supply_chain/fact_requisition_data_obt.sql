{{
     config(
         materialized = "table",
         tags=["supply_chain"]
     )

}}

SELECT
    *,
    DATEDIFF(day, milestone_actual_date, actual_date_new) as milestone_lead_time
FROM
(
    SELECT DISTINCT
        proj_no	 as project_id,
        proj_id	 as project_code,
        ver	as current_revision,
        title	as requisition_title,
        req_originator	as Originator,
        buyer	,
        expediter	,
        criticality	,
        discipline	,
        supplier	,
        potential_suppliers	,
        budget	,
         CASE
            WHEN po_gross_value IS NOT NULL then 
            CAST(REPLACE(SPLIT_PART(TRIM(po_gross_value),' ',1), ',', '') AS DECIMAL(18, 2))
            ELSE NULL END AS commitment_value,
        proj_planned_tmr_hdr_id	as requistion_no,
        po_number	,
        deliv_deadline	as ros_date,
        lead_time	,
        sub_project	,
        cf_new_float	as float,
        proj_no||'-'||proc_mstone_id||'-'||calc_seq as Milestone_id,
        proc_mstone_id as Milestone_name	,
        m_planned_date	as milestone_planned_date,
        m_forecast_date	as milestone_forecast_date,
        m_actual_date	as milestone_actual_date,
        m_duration	as milestone_duration,
        m_float	as milestone_float,
        milestone_comment	,
        calc_seq	as milestone_sequence,
        remarks	as comment,
        contract_owner	 ,
        contract_holder	,
        project_region	,
        customer_po_no	as customer_po,
        rr_discipline	,
        client_ref_no	as client_reference_no,
        CASE
        when m_actual_date is not null then 'Completed'
        when m_forecast_date >= current_date
        and m_forecast_date > m_planned_date then 'Planned'
        when m_forecast_date >= current_date then 'Planned'
        when m_forecast_date is not null
        and m_planned_date is not null
        and m_forecast_date < current_date then 'Overdue'
        when m_planned_date is null then 'No Planned Date'
        else null end as status,
        CASE
        when calc_seq = 31000 then deliv_deadline
        else null end as ros_date_new,
        LEAD(m_actual_date, 1) over (
            ORDER BY
                proj_id,
                proj_planned_tmr_hdr_id,
                calc_seq ASC
        ) as actual_date_new,
        etl_load_date as dataset_refreshed_date
    FROM
        {{ source('supply_chain_dim', 'cext_w_psr_st_api_dh') }}
)