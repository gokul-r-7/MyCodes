{{
     config(
         materialized = "table",
         tags=["supply_chain"]
     )

}}

SELECT *,
CASE
WHEN
DATEDIFF(day,gl_date,invoice_due_date) >0 then 'Late'
WHEN
DATEDIFF(day,gl_date,invoice_due_date) <=0 then 'On Time'
else null end as payment_status
from (
SELECT DISTINCT
    ap.supplier_num,
    ap.operating_unit,
    ap.invoice_num,
    CASE
        WHEN ap.gl_date = ''
        or ap.gl_date is null then null
        else TO_DATE(ap.gl_date, 'DD-Mon-YY')::Date
    end as gl_date,
    ap.payment_curr,
    ap.terms,
    ap.purchase_invoice_number,
    CASE
        when ap.invoice_creation_date = ''
        or ap.invoice_creation_date is null then null
        else TO_DATE(ap.invoice_creation_date, 'DD-Mon-YY')::Date
    end as invoice_creation_date,
    CASE
        when ap.invoice_receipt_date = ''
        or ap.invoice_receipt_date is null then null
        else TO_DATE(ap.invoice_receipt_date, 'DD-Mon-YY')::Date
    end as invoice_receipt_date,
    CASE
        when ap.invoice_due_date = ''
        or ap.invoice_due_date is null then null
        else TO_DATE(ap.invoice_due_date, 'DD-Mon-YY')::Date
    end as invoice_due_date,
    ap.user_name_invoice_last_update,
    ap.payment_amt_in_document_currency,
    CASE
        when ap.payment_curr = 'USD' then 1
        else cre.conversion_rate end as usd_conv,
    CASE
        when ap.void_date = ''
        or ap.void_date is null then null
        else TO_DATE(ap.void_date, 'DD-Mon-YY')::Date
    end as void_date,
    ap.payables_org_name,
    sup.supplier_name,
    sup.supplier_type,
    sup.supplier_number,
    le.region,
    le.country,
    CAST(ap.etl_load_date as date) as data_refreshed_date
FROM {{ source('finance', 'gbs_ap_po') }} ap
    LEFT JOIN {{ source('finance', 'gbs_suppliers') }} sup ON ap.supplier_num = sup.supplier_number
    LEFT JOIN {{ source('finance', 'master_listing_legal_entity') }} le ON le.le = ap.entity_code
    LEFT JOIN (
        with currency_exchange_rate as (
            SELECT from_currency,
                to_currency,
                conversion_rate,
                TO_DATE(month_start_date, 'DD-MM-YYYY') as month_start_date,
                row_number() OVER (
                    partition by from_currency,
                    to_currency
                    order by TO_DATE(month_start_date, 'DD-MM-YYYY') DESC
                ) as rn
            from {{ source('finance', 'currency_exchange_rate') }}
        )
        SELECT from_currency,
            to_currency,
            conversion_rate
        from currency_exchange_rate
        where rn = 1
    ) cre ON ap.payment_curr || '_USD' = cre.from_currency || '_' || cre.to_currency
)