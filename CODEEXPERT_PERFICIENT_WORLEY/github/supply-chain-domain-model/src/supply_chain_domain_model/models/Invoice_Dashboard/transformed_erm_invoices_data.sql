{%- set run_date = "CURRENT_TIMESTAMP()" -%}


{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='false', 
        custom_location=target.location ~ 'transformed_erm_invoices_data/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}



with po_det as (
    select
        proj.proj_no,
        proj.proj_id,
        proj.proj_id || ' - ' || proj.descr as proj_descr,
        po_hdr.po_no,
        po_hdr.po_id,
        po_hdr.title as description,
        po_hdr.ver as highest_issued_rev,
        phc.sub_project,
        suppl.suppl_id as supplier_no,
        suppl.name_1 as supplier,
        phc.cust_po_no,
        po_hdr.issue_date,
        po_hdr.merc_hand_usr_id as buyer,
        npip.po_gross_value as po_commitment_value,
        po_hdr.cur_id as commited_currency,
        phc.tax_status,
        cast(po_hdr.execution_date as date) as etl_load_date
    from {{ source('curated_erm', 'po_hdr') }} po_hdr
    left outer join {{ ref('transformed_erm_suppl') }} suppl on po_hdr.suppl_no = suppl.suppl_no
    left outer join {{ source('curated_erm', 'proj') }} proj on po_hdr.proj_no = proj.proj_no
    left outer join {{ source('curated_erm', 'po_hdr_cpv') }} phc on po_hdr.po_no = phc.po_no
    left outer join {{ ref('transformed_net_po_issd_price') }} npip on npip.po_no = po_hdr.po_no and npip.ver = po_hdr.ver
    where 
        po_hdr.is_current = 1 and 
        po_hdr.proc_int_stat_no = 3 and
        proj.is_current = 1 and
        phc.is_current = 1

    union all

    select
        proj.proj_no,
        proj.proj_id,
        proj.proj_id || ' - ' || proj.descr as proj_descr,
        po_hdr.po_no,
        po_hdr.po_id,
        po_hdr.title as description,
        po_hdr.ver as highest_issued_rev,
        phc.sub_project,
        suppl.suppl_id as supplier_no,
        suppl.name_1 as supplier,
        phc.cust_po_no,
        po_hdr.issue_date,
        po_hdr.merc_hand_usr_id as buyer,
        npip.po_gross_value as po_commitment_value,
        po_hdr.cur_id as commited_currency,
        phc.tax_status,
        cast(po_hdr.execution_date as date) as etl_load_date
    from {{ source('curated_erm', 'po_hdr_hist') }} po_hdr
    left outer join  {{ ref('transformed_erm_suppl') }} suppl on po_hdr.suppl_no = suppl.suppl_no
    left outer join {{ source('curated_erm', 'proj') }} proj on po_hdr.proj_no = proj.proj_no
    left outer join {{ source('curated_erm', 'po_hdr_cpv') }} phc on po_hdr.po_no = phc.po_no
    left outer join {{ ref('transformed_net_po_issd_price') }} npip on npip.po_no = po_hdr.po_no and npip.ver = po_hdr.ver 
    where 
        po_hdr.is_current = 1
        and proj.is_current = 1
        and phc.is_current = 1
        and po_hdr.ver = (
            select max(hh2.ver)
            from {{ source('curated_erm', 'po_hdr_hist') }} hh2
            where hh2.po_no = po_hdr.po_no
            and hh2.is_current = 1
            and not exists (
                select null
                from {{ source('curated_erm', 'po_hdr') }} h3
                where h3.po_no = hh2.po_no
                and h3.ver = hh2.ver
                and h3.proc_int_stat_no <> 3
                and h3.is_current = 1
            )
        )
        and not exists (
            select 1 
            from {{ source('curated_erm', 'po_hdr') }} ph
            where ph.po_no = po_hdr.po_no
            and ph.ver = po_hdr.ver
            and ph.proc_int_stat_no = 3
            and ph.is_current = 1
        )
)


,to_inv_c as (
    select 
        sum(ii.amount) as tot_invoice_to_commitment,
        ih.po_no
    from 
        {{ source('curated_erm', 'inv_hdr') }} ih
    left outer join 
        {{ source('curated_erm', 'inv_item') }} ii on ih.inv_hdr_no = ii.inv_hdr_no
    left outer join
        {{ source('curated_erm', 'vat_type') }} vt on ii.vat_type_no = vt.vat_type_no
    left outer join
        {{ source('curated_erm', 'po_hdr') }} ph on ih.po_no = ph.po_no
    where 
        vt.vat_type_id = 'no tax'
        and (ii.po_item_no is not null or ii.po_pay_plan_no is not null)
        and ih.is_current = 1
        and ii.is_current = 1
        and vt.is_current = 1
        and ph.is_current = 1
    group by 
        ih.po_no
)



,tot_tax_fr as 
(
    select sum(tax_and_freight) as tot_inv_tax_and_freight
          ,po_no
    from 
    (
        select ii.amount as tax_and_freight
               ,ih.po_no
               ,ih.inv_hdr_no
        from  {{ source('curated_erm', 'inv_hdr') }} ih
        left outer join {{ source('curated_erm', 'inv_item') }} ii 
            on ih.inv_hdr_no = ii.inv_hdr_no 
            and ii.is_current = 1 
            and ii.po_item_no is null
        left outer join {{ source('curated_erm', 'vat_type') }} vt 
            on ii.vat_type_no = vt.vat_type_no 
            and vt.is_current = 1
        left outer join {{ source('curated_erm', 'po_item') }} pi 
            on ii.po_item_no = pi.po_item_no 
            and pi.is_current = 1
        where ih.is_current = 1
        and not exists (
            select 1 
            from {{ source('curated_erm', 'inv_item') }} ii1 
            where ii1.po_pay_plan_no is not null 
            and ii.inv_item_no = ii1.inv_item_no
        )
    )
    group by po_no
)



,inv_det as (
    select  
        ih.po_no,
        ih.inv_hdr_no,
        ih.suppl_inv_id,
        ih.suppl_inv_date as invoice_date,
        ih.receive_date as received,
        ih.approve_date,
        ih.approver as approved_by,
        ihc.ora_supplier_no || ' - ' || ihc.ora_supplier_site as acc_supp_and_loc,
        pt.pay_term_id,
        ih.pay_date,
        invh.inv_applied_to_commitment,
        invi.tax_and_freight,
        ih.inv_amount as invoice_amount,
        pa.paid_amount as payment_amount, 
        pa.pay_date as payment_date,
        ip.inv_pay_id as payment_number,
        case 
            when ih.inv_hdr_stat_no = 1 then 'created'
            when ih.inv_hdr_stat_no = 2 then 'awaiting approval'
            when ih.inv_hdr_stat_no = 3 then 'issued'
            when ih.inv_hdr_stat_no = 4 then 'transferred'
            when ih.inv_hdr_stat_no = 5 then 'reopened'
            when ih.inv_hdr_stat_no = 6 then 'rejected in approval'
            when ih.inv_hdr_stat_no = 7 then 'transfer accepted'
            when ih.inv_hdr_stat_no = 8 then 'transfer rejected'
            when ih.inv_hdr_stat_no = 9 then 'paid'
            else null
        end as status
    from {{ source('curated_erm', 'inv_hdr') }} ih
    left join {{ source('curated_erm', 'inv_hdr_cpv') }} ihc 
        on ih.inv_hdr_no = ihc.inv_hdr_no and ihc.is_current = 1
    left join {{ source('curated_erm', 'pay_term') }} pt 
        on ih.pay_term_no = pt.pay_term_no and pt.is_current = 1
    left join (
        select 
            ih.po_no,
            ih.inv_hdr_no,
            sum(ii.amount) as inv_applied_to_commitment
        from {{ source('curated_erm', 'inv_hdr') }} ih
        inner join {{ source('curated_erm', 'inv_item') }} ii 
            on ih.inv_hdr_no = ii.inv_hdr_no
        inner join {{ source('curated_erm', 'vat_type') }} vt 
            on ii.vat_type_no = vt.vat_type_no
        inner join {{ source('curated_erm', 'po_hdr') }} ph 
            on ih.po_no = ph.po_no
        where vt.vat_type_id = 'no tax'
            and (ii.po_item_no is not null or ii.po_pay_plan_no is not null)
            and ih.is_current = 1
            and ii.is_current = 1
            and vt.is_current = 1
            and ph.is_current = 1
        group by ih.po_no, ih.inv_hdr_no
    ) invh on ih.inv_hdr_no = invh.inv_hdr_no
    left join (
        select 
            ih.po_no,
            ih.inv_hdr_no,
            sum(ii.amount) as tax_and_freight
        from {{ source('curated_erm', 'inv_hdr') }} ih
        inner join {{ source('curated_erm', 'inv_item') }} ii 
            on ih.inv_hdr_no = ii.inv_hdr_no
        left join {{ source('curated_erm', 'vat_type') }} vt 
            on ii.vat_type_no = vt.vat_type_no
        left join {{ source('curated_erm', 'po_item') }} pi 
            on ii.po_item_no = pi.po_item_no
        left join {{ source('curated_erm', 'inv_item') }} ii1 
            on ii.inv_item_no = ii1.inv_item_no 
            and ii1.po_pay_plan_no is not null
        where ii.po_item_no is null
            and ii1.inv_item_no is null
            and ih.is_current = 1
            and ii.is_current = 1
            and vt.is_current = 1
            and pi.is_current = 1
        group by ih.po_no, ih.inv_hdr_no
    ) invi on ih.po_no = invi.po_no and ih.inv_hdr_no = invi.inv_hdr_no
    left join (
        select 
            inv_hdr.inv_hdr_no,
            cast('3333' as DECIMAL(38,2)) as paid_amount,--this is temporarily harcoded.get the logic from ms for sp
            min(inv_pay.pay_date) as pay_date
        from {{ source('curated_erm', 'inv_hdr') }} inv_hdr
        left join {{ source('curated_erm', 'inv_pay') }} inv_pay
            on inv_hdr.inv_hdr_no = inv_pay.inv_hdr_no
        where inv_hdr.is_current = 1
            and inv_pay.is_current = 1
        group by inv_hdr.inv_hdr_no
    ) pa on ih.inv_hdr_no = pa.inv_hdr_no
    left join (
        select 
            ip1.inv_hdr_no,
            concat_ws(',', collect_list(ip1.inv_pay_id)) as inv_pay_id
        from {{ source('curated_erm', 'inv_pay') }} ip1
        where ip1.is_current = 1
        group by ip1.inv_hdr_no
    ) ip on ih.inv_hdr_no = ip.inv_hdr_no
    where ih.is_current = 1
)



,r_tot as (
    select 
        proj_no,
        po_no,
        po_id,
        sum(total_received) as received_total
    from (
        select 
            phh.proj_no,
            phh.po_no,
            phh.po_id,
            phh.ver,
            ri.receive_unit_quan,
            pih.po_item_no,
            pih.price_quan,
            pih.price,
            ri.receive_unit_quan * pih.price as total_received
        from {{ source('curated_erm', 'receipt_hdr') }} rh
        left join {{ source('curated_erm', 'po_hdr_hist') }} phh 
            on phh.po_no = rh.po_no
            and phh.is_current = 1
        left join {{ source('curated_erm', 'po_hdr_hist') }} phh1 
            on phh.po_no = phh1.po_no
            and phh1.is_current = 1
        left join {{ source('curated_erm', 'po_item_hist') }} pih 
            on phh1.po_hdr_hist_no = pih.po_hdr_hist_no
            and pih.is_current = 1
        left join {{ source('curated_erm', 'receipt_item') }} ri 
            on rh.receipt_hdr_no = ri.receipt_hdr_no
            and ri.is_current = 1
        where 
            rh.receipt_stat_no = 6
            and pih.ver = (
                select max(pih2.ver)
                from {{ source('curated_erm', 'po_item_hist') }} pih2
                where 
                    pih2.po_item_no = pih.po_item_no
                    and pih2.ver <= phh.ver
                    and pih2.is_current = 1
            )    
            and phh.ver = (
                select max(hh2.ver)
                from {{ source('curated_erm', 'po_hdr_hist') }} hh2
                where 
                    hh2.po_no = phh.po_no
                    and hh2.is_current = 1
                    and not exists (
                        select 1
                        from {{ source('curated_erm', 'po_hdr') }} h3
                        where 
                            h3.po_no = hh2.po_no
                            and h3.ver = hh2.ver
                            and h3.is_current = 1
                            and h3.proc_int_stat_no <> 3
                    )
            )
    )
    group by proj_no, po_no, po_id
)


,cr_dr as (
    select 
        proj_no,
        po_no,
        po_id,
        sum(credit_bal) as credit_bal,
        sum(debit_bal) as debit_bal
    from (
        select 
            phh.proj_no,
            phh.po_no,
            phh.po_id,
            phh.ver,
            pih.price,
            pih.quan,
            case 
                when pih.price < 0 then nvl(pih.quan, 0) * abs(pih.price) 
                else 0 
            end as credit_bal,
            case 
                when pih.price > 0 then nvl(pih.quan, 0) * abs(pih.price) 
                else 0 
            end as debit_bal
        from {{ source('curated_erm', 'po_hdr_hist') }} phh
        left join {{ source('curated_erm', 'po_hdr_hist') }} phh1 
            on phh.po_no = phh1.po_no
            and phh1.is_current = 1
        left join {{ source('curated_erm', 'po_item_hist') }} pih 
            on phh1.po_hdr_hist_no = pih.po_hdr_hist_no
            and pih.is_current = 1
        where 
            pih.mat_grp_type_no = 3
            and pih.ver = (
                select max(pih2.ver)
                from {{ source('curated_erm', 'po_item_hist') }} pih2
                where 
                    pih2.po_item_no = pih.po_item_no
                    and pih2.ver <= phh.ver
                    and pih2.is_current = 1
            )    
            and phh.ver = (
                select max(hh2.ver)
                from {{ source('curated_erm', 'po_hdr_hist') }} hh2
                where 
                    hh2.po_no = phh.po_no
                    and hh2.is_current = 1
                    and not exists (
                        select 1
                        from {{ source('curated_erm', 'po_hdr') }} h3
                        where 
                            h3.po_no = hh2.po_no
                            and h3.ver = hh2.ver
                            and h3.is_current = 1
                            and h3.proc_int_stat_no <> 3
                    )
            )
    )
    group by 
        proj_no,
        po_no,
        po_id
)


select 
    po_det.proj_no,
    po_det.proj_id,
    po_det.proj_descr,
    po_det.po_no,
    po_det.po_id,
    po_det.description,
    po_det.highest_issued_rev,
    po_det.sub_project,
    po_det.supplier_no,
    po_det.supplier,
    po_det.cust_po_no,
    tax_st.value as tax_status,
    po_det.issue_date,
    po_det.buyer,
    po_det.po_commitment_value,
    po_det.commited_currency,
    to_inv_c.tot_invoice_to_commitment,
    tot_tax_fr.tot_inv_tax_and_freight,
    to_inv_c.tot_invoice_to_commitment + coalesce(tot_tax_fr.tot_inv_tax_and_freight, 0) as total_invoices_applied,
    inv_det.suppl_inv_id as invoice_number,
    inv_det.invoice_date,
    inv_det.received,
    inv_det.approve_date,
    inv_det.approved_by,
    inv_det.acc_supp_and_loc,
    inv_det.pay_term_id as payment_terms,
    inv_det.pay_date as payment_due_date,
    inv_det.inv_applied_to_commitment,
    inv_det.tax_and_freight,
    inv_det.invoice_amount,
    inv_det.payment_amount,
    inv_det.payment_date,
    inv_det.payment_number,
    inv_det.status,
    case when inv_det.inv_hdr_no is null then 1 else 0 end as po_without_inv,
    case when ppp.po_no is null then 1 else 0 end as po_pay_plan_flg,
    r_tot.received_total - coalesce(cr_dr.credit_bal, 0) as received_total,
    usr.name as buyer_name,
    po_det.etl_load_date,
    {{run_date}} as model_created_date,
    {{run_date}} as model_updated_date,
    {{ generate_load_id(model) }} as model_load_id	
from  po_det
left join to_inv_c on po_det.po_no = to_inv_c.po_no
left join tot_tax_fr on po_det.po_no = tot_tax_fr.po_no
left join inv_det on po_det.po_no = inv_det.po_no
left join (select distinct proj_no, po_no from {{ source('curated_erm', 'po_pay_plan') }} where is_current = 1) ppp on po_det.po_no = ppp.po_no and po_det.proj_no = ppp.proj_no
left join {{ source('curated_erm', 'cusp_value_list_item') }} tax_st on po_det.tax_status = tax_st.value_list_item_no and tax_st.is_current = 1
left join r_tot on po_det.po_no = r_tot.po_no
left join cr_dr on po_det.po_no = cr_dr.po_no
left join {{ source('curated_erm', 'usr') }} usr on po_det.buyer = usr.usr_id and usr.is_current = 1