
SELECT
    a.wbs,
    a.cwa,
    a.cwpzone,
    a.clientdocno,
    a.stvval_wor_status_proc_pipe,
    a.pidref,
    a.unique_line_id,
    a.workpackno,
    a.progresspercent  as cumpercent,
    a.statuslevel,
    a.search_key,
    a.bore_in,
    a.zone,
    a.name,
    a.holdflag,
    a.ispe,
    a.tspe,
    a.pspe,
    a.duty,
    a.status,
    a.execution_date as fact_snapshot_date,
    SUM(
        CASE
        WHEN right(b.length, 2) = 'in' THEN rtrim(replace(b.length, 'in', ''))
        WHEN length(b.length) = 0 THEN '0.00'
        ELSE b.length end
    ) as length,

    b.spref,
    b.pipe,
    b.type,
    CASE
        WHEN b.type = 'VALV' AND POSITION('RF' IN b.description)>0 THEN 'Flanged Valves'
        WHEN b.type = 'VALV' AND POSITION('FF' IN b.description)>0 THEN 'Flanged Valves'
        WHEN b.type = 'VALV' AND POSITION('SW' IN b.description)>0 THEN 'Welded Valves'
        WHEN b.type = 'VALV' AND POSITION('BE' IN b.description)>0 THEN 'Welded Valves'
        WHEN b.type= 'VALV' THEN 'Valve'
        ELSE NULL
        END as valve,
    SUM(
        CASE
        WHEN right(b.nb1, 2) = 'in' THEN rtrim(replace(b.nb1, 'in', ''))
        WHEN length(b.nb1) = 0 THEN '0.00'
        ELSE nb1 end
    ) as Pipe_diameter,
    b.iso,
    b.description,
    b.material,
    b."tag" as tag1,
    split_part(b.spref,'/',2) as piping_specification,
    b.execution_date as pipecomp_snapshot_date
FROM
    {{ref('fact_e3d_pipes')}} a
    LEFT JOIN {{ ref('dim_e3d_pipecompwbsrep') }} b ON a.name || split_part(a.execution_date, ' ', 1) = CASE
    WHEN LEFT(b.pipe, 1) = '/' then substring(b.pipe, 2, length(b.pipe))
    else b.pipe end || b.execution_date
GROUP BY
    a.wbs,
    a.cwa,
    a.cwpzone,
    a.clientdocno,
    a.stvval_wor_status_proc_pipe,
    a.pidref,
    a.unique_line_id,
    a.workpackno,
    a.progresspercent,
    a.statuslevel,
    a.search_key,
    a.bore_in,
    a.zone,
    a.name,
    a.holdflag,
    a.ispe,
    a.tspe,
    a.pspe,
    a.duty,
    a.status,
    fact_snapshot_date,
    b.spref,
    b.pipe,
    b.type,
    valve,
    b.iso,
    b.description,
    b.material,
    tag1,
    piping_specification,
    pipecomp_snapshot_date
