{{
     config(
         materialized = "table",
         tags=["supply_chain"]
     )

}}


SELECT
    *,
    CASE
    when pds <= 1
    or pds = 0 then 'On Time'
    when pds > 1 then 'Late'
    else 'In Progress' end AS pds_status,
    CASE
    when ros <= 1
    or ros = 0 then 'On Time'
    when ros > 1 then 'Late'
    else 'In Progress' end AS ros_status,
    CASE
    when cdd <= 1
    or cdd = 0 then 'On Time'
    when cdd > 1 then 'Late'
    else 'In Progress' end AS cdd_status,
    CASE
    when received_date < po_issue_date then 'Negative Date'
    when received_date < po_creation_date then 'Negative Date'
    when received_date > CURRENT_DATE then 'Received Date in the future'
    else 'Consistent Date' end AS received_date_issue,
    CASE
    when cdd_status = 'On Time'
    or cdd_status = 'Late'
    or pds_status = 'On Time'
    or pds_status = 'Late'
    or ros_status = 'On Time'
    or ros_status = 'Late' then 'Received'
    else 'Open' end AS received_open,
    CASE
    when po_creation_date is null
    or req_creation_date is null then null
    else DATEDIFF(day, req_creation_date, po_creation_date) end AS req_processing_time,
    EXTRACT(
        year
        FROM
            received_date
    ) AS received_year,
    TO_CHAR(received_date, 'Mon') + ' ' + TO_CHAR(received_date, 'YYYY') AS received_month,
    TO_NUMBER(TO_CHAR(received_date, 'YYYYMMDD'), '99999999') as received_date_key,
    EXTRACT(
        year
        FROM
            po_creation_date
    ) AS po_creation_year,
    TO_CHAR(po_creation_date, 'Mon') + ' ' + TO_CHAR(po_creation_date, 'YYYY') AS po_creation_month,
    TO_NUMBER(TO_CHAR(po_creation_date, 'YYYYMMDD'), '99999999') as po_creation_date_key,
    EXTRACT(
        year
        FROM
            po_issue_date
    ) AS po_issue_year,
    TO_CHAR(po_issue_date, 'Mon') + ' ' + TO_CHAR(po_issue_date, 'YYYY') AS po_issue_month,
    TO_NUMBER(TO_CHAR(po_issue_date, 'YYYYMMDD'), '99999999') as po_issue_date_key,
    po_line_quantity * line_item_unit_price * usd_conv as usd_line_total_conv,
    line_item_unit_price * usd_conv as usd_line_unit_conv,
    CASE
    when usd_line_total_conv < 0.0001 then 1
    else 0 end as po_total_conv,
    CASE
    when po_line_quantity * line_item_unit_price * usd_conv < 10000 then '<10K'
    when po_line_quantity * line_item_unit_price * usd_conv >= 10000
    AND po_line_quantity * line_item_unit_price * usd_conv < 100000 then '10K-100K'
    when po_line_quantity * line_item_unit_price * usd_conv >= 100000
    AND po_line_quantity * line_item_unit_price * usd_conv <= 1000000 then '100K-1M'
    else '>1M' end as po_range_filter,
    po_number || '_' || po_line_number as po_and_line_item_no,
    'Refreshed Date: ' + TO_CHAR(
        TO_DATE(data_refreshed_date, 'YYYY-MM-DD'),
        'DD-Mon-YYYY'
    ) AS refreshed_date,
    GETDATE() as dag_execution_date
FROM
    (
        SELECT
            DISTINCT 
	    poh.project_number as project_id,
            poh.po_number,
            poh.worley_office_origin,
            poh.po_creation_date,
            poh.title,
            poh.gross_value,
            poh.client,
            poh.supplier,
            poh.supplier_code,
            poh.supplier_location,
            poh.po_issue_date,
            poh.buyer_name as buyer,
            poh.database_instance_name,
            pol.cost_type,
            pol.required_on_site_date,
            pol.purchase_order_line_item_internal_id,
            pol.po_line_number,
            pol.po_line_quantity,
            pol.line_item_unit_price,
            ROUND(pol.po_line_quantity * pol.line_item_unit_price) as line_item_total_price,
            pol.promised_date,
            pol.incoterm_tax_terms,
            pol.coa as code_of_account,
            pol.item_tag,
            pol.item_description,
            pol.po_revision,
            pol.contract_delivery_date,
            pol.expenditure_item_date as req_creation_date,
            por.received_date,
            por.received_quantity,
            por.po_status,
            CAST(poh.etl_load_date as date) as data_refreshed_date,
            DATEDIFF(day, pol.promised_date, por.received_date) as pds,
            DATEDIFF(day, pol.required_on_site_date, por.received_date) as ros,
            DATEDIFF(day, pol.contract_delivery_date, por.received_date) as cdd,
            DATEDIFF(day, poh.po_issue_date, por.received_date) as polt_v1,
            DATEDIFF(day, poh.po_creation_date, por.received_date) as polt_v2,
            CASE
            when poh.po_creation_date is null
            or poh.po_issue_date is null then null
            else DATEDIFF(day, poh.po_creation_date, poh.po_issue_date) end AS po_approval_time,
            case
            when por.po_status = 'Ready/Issued'
            or por.po_status = 'Create/Change' then 1
            else 0 end as po_stts_flag,
            CASE
            when received_date >= '2020-01-01'
            and received_date <= '2040-12-31'
            or received_date is null then 1
            else 0 end as received_flag,
            pol.currency_type,
            CASE
            when pol.currency_type = 'USD' then 1
            else cre.conversion_rate end as usd_conv,
            pol.financial_account_code as unspsc,
            spc.supplier_name as erm_supplier,
            spc.country as supplier_country,
            prol.worley_project_region,
            prol.sub_region as project_sub_region,
            prol.country as project_country,
            prol.worley_project_office,
            prol.worley_csg as sector,
            prol.client as client_country,
            prol.buy_on_whose_paper as paper,
            spl.name_1 as catg_supplier,
            spl.country_id as catg_country_code,
            dic.name as erm_supplier_country,
            tam.group_l1,
            tam.category_l2,
            tam.sub_category_l3,
            tam.family_l4,
            tam.commodity_l5,
            CASE
            when psr.sub_project = '' then NULL
            else sub_project end as sub_project,
            CASE
            when tam.category_l2 is null then '00 Not Classified'
            else tam.category_l2 end as ctg_category_l2,
            CASE
            when tam.sub_category_l3 is null then '00 Not Classified'
            else tam.sub_category_l3 end as ctg_sub_category_l3,
            CASE
            when tam.family_l4 is null then '00 Not Classified'
            else tam.family_l4 end as ctg_family_l4,
            CASE
            when tam.commodity_l5 is null then '00 Not Classified'
            else tam.commodity_l5 end as ctg_commodity_l5
        FROM
            {{ source('supply_chain_dim', 'erm_po_header') }} poh
            LEFT JOIN (
                SELECT
                    po_internal_id,
                    purchase_order_line_item_internal_id,
                    cost_type,
                    required_on_site_date,
                    po_line_number,
                    po_line_quantity,
                    line_item_unit_price,
                    promised_date,
                    incoterm_tax_terms,
                    coa,
                    item_tag,
                    item_description,
                    po_revision,
                    contract_delivery_date,
                    expenditure_item_date,
                    currency_type,
                    financial_account_code,
                    ROW_NUMBER() OVER (
                        PARTITION BY po_internal_id,
                        purchase_order_line_item_internal_id
                        ORDER BY
                            cost_type,
                            required_on_site_date,
                            po_line_number,
                            po_line_quantity,
                            line_item_unit_price,
                            promised_date,
                            incoterm_tax_terms,
                            coa,
                            item_tag,
                            item_description,
                            po_revision,
                            contract_delivery_date,
                            expenditure_item_date,
                            currency_type,
                            financial_account_code
                    ) as rn
                FROM
                    {{ source('supply_chain_dim', 'erm_po_line_item') }}
            ) pol ON poh.purchase_order_internal_id = pol.po_internal_id
            and pol.rn = 1
            LEFT JOIN (
                SELECT
                    po_line_item_internal_id,
                    received_date,
                    received_quantity,
                    po_status,
                    ROW_NUMBER() OVER (
                        PARTITION BY po_line_item_internal_id
                        ORDER BY
                            received_date,
                            received_quantity,
                            po_status
                    ) as rn
                FROM
                    {{ source('supply_chain_dim', 'erm_po_reciepts') }}
            ) por ON pol.purchase_order_line_item_internal_id = por.po_line_item_internal_id
            and por.rn = 1
            LEFT JOIN (
                SELECT
                    supplier_no,
                    supplier_name,
                    country,
                    ROW_NUMBER() OVER (
                        PARTITION BY supplier_no
                        ORDER BY
                            supplier_name,
                            country
                    ) as rn
                FROM
                    {{ source('supply_chain_dim', 'erm_supplier_country') }}
            ) spc ON poh.supplier_code = spc.supplier_no
            AND spc.rn = 1
            LEFT JOIN (
                SELECT
                    suppl_id,
                    name_1,
                    country_id,
                    ROW_NUMBER() OVER (
                        PARTITION BY suppl_id
                        ORDER BY
                            name_1,
                            country_id
                    ) as rn
                FROM
                    {{ source('supply_chain_dim', 'erm_suppl') }}
            ) spl ON poh.supplier_code = spl.suppl_id
            and spl.rn = 1
            INNER JOIN (
                SELECT
                    country_id,
                    name,
                    ROW_NUMBER() OVER (
                        PARTITION BY country_id
                        ORDER BY
                            name
                    ) as rn
                from
                    {{ source('supply_chain_dim', 'erm_dim_country') }}
            ) dic ON spl.country_id = dic.country_id
            and dic.rn = 1
            LEFT JOIN (
                SELECT
                    project_id,
                    worley_project_region,
                    sub_region,
                    country,
                    worley_project_office,
                    worley_csg,
                    client,
                    buy_on_whose_paper,
                    ROW_NUMBER() OVER (
                        PARTITION BY project_id
                        ORDER BY
                            worley_project_region,
                            sub_region,
                            country,
                            worley_project_office,
                            worley_csg,
                            client
                    ) as rn
                from
                    {{ source('supply_chain_dim', 'erm_project_location') }}
            ) prol ON poh.project_number = prol.project_id
            and prol.rn = 1
            LEFT JOIN (
                SELECT
                    po_number,
                    sub_project,
                    ROW_NUMBER() OVER (
                        PARTITION BY po_number
                        ORDER BY
                            sub_project
                    ) as rn
                FROM
                    {{ source('supply_chain_dim', 'cext_w_psr_st_api_dh') }}
            ) psr ON psr.po_number = poh.po_number
            AND psr.rn = 1
            LEFT JOIN (
                        SELECT
                            unspsc,
                            group_l1,
                            category_l2,
                            sub_category_l3,
                            family_l4,
                            commodity_l5,
                            ROW_NUMBER() OVER (
                                PARTITION BY unspsc
                                ORDER BY
                                    group_l1,
                                    category_l2,
                                    sub_category_l3,
                                    family_l4,
                                    commodity_l5
                            ) as rn
                        FROM
                            {{ source('supply_chain_dim', 'erm_taxonomy_master') }}
                    )
			tam ON tam.unspsc = pol.financial_account_code
            LEFT JOIN (
                with currency_exchange_rate as (
                    SELECT
                        from_currency,
                        to_currency,
                        conversion_rate,
                        TO_DATE(month_start_date, 'DD-MM-YYYY') as month_start_date,
                        row_number() OVER (
                            partition by from_currency,
                            to_currency
                            order by
                                TO_DATE(month_start_date, 'DD-MM-YYYY') DESC
                        ) as rn
                    from
                        {{ source('finance', 'currency_exchange_rate') }}
                )
                SELECT
                    from_currency,
                    to_currency,
                    conversion_rate
                from
                    currency_exchange_rate
                where
                    rn = 1
            ) cre ON pol.currency_type + '_USD' = cre.from_currency + '_' + cre.to_currency
    )