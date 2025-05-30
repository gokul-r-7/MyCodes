{%- set run_date = "CURRENT_TIMESTAMP()" -%}
{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_e3d_gensec_floor_union/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}


select  
    anty,
    bang_degree,
    bevcod,
    buil,
    commcode,
    ctye,
    ctys,
    cut_length_in,
    cwarea,
    cwarea_of_stru,
    cwp,
    desc,
    desp,
    discipline,
    drgp,
    erel,
    ewp,
    fire,
    fireproofing,
    frmw,
    func,
    grade,
    gridno,
    gtyp,
    guid,     
    inre,
    insulation,
    insutype,
    ispe,
    itemno,
    jlne,
    jlns,
    joie,
    jois,
    jusl,
    lmirr,
    lock,
    material,
    matr,
    meml,
    mps_comment,
    mps_date_piping_placeholder_modeling_completed,
    mps_date_piping_stress_loads_to_stru,
    mps_date_structural_engineering_actual,
    mps_date_structural_engineering_complete,
    mps_date_structural_engineering_forecast,
    mps_date_structural_modeling_complete,
    mps_foundation_req,
    mps_isometric_number,
    mps_line_number,
    mps_structural_engineering_released_to_piping,
    NAME,
    nett_volume_in3,
    numb,
    ori,
    owner,
    parnam,
    piecemark,
    pipecompression,
    piletension,
    pos,
    posno,
    prodrf,
    purp,
    repcou,
    section_name,
    shop,
    site,
    spre,
    srel,
    steeltype,
    stru,
    strucate,
    strucl,
    structural_mps_number,
    strutype,
    STVVAL,
    subsystem,
    tagno,
    tekla_guid,
    tekla_ifcguid,
    tekla_material,
    tekla_profile,
    tekla_version,
    tekla_warning,
    tmrref,
    type,
    '' as type1,     
    units,
    uom,
    welddp_in,
    worktype,
    zone,
    catalogue_reference,     
    cast(extracted_date as date) as extracted_date,
    reference_of_the_element,     
    nett_weight,     
    stlweightclass,     
    source_system_name,
    project_code,
    Site1,
    MTO_Type1,
    WBS, 
    CWA,
    CWPZONE,
    CWP1,
	PROJECT_CODE_WBS_CWA_CWPZONE,
    {{run_date}} as model_created_date,
    {{run_date}} as model_updated_date,
    {{ generate_load_id(model) }} as model_load_id
FROM
(
SELECT 
        NAME,
        'GENSEC' as type,
        lock,
        owner,
        desc,
        func,
        gtype as gtyp,
        numb,
        desp,
        strucl,
        buil,
        shop,
        bang_degree,
        lmirr,
        jois,
        joie,
        jlns,
        jlne,
        jusl,
        ctys,
        ctye,
        srel,
        erel,
        grade,
        matr,
        ispe,
        fire,
        welddp_in,
        posno,
        parnam,
        inre,
        spre,
        anty,
        drgp,
        meml,
        pos,
        ori,
        purp,
        tmrref,
        repcou,
        prodrf,
        bevcod,
        mps_line_number,
        mps_isometric_number,
        mps_date_piping_placeholder_modeling_completed,
        mps_date_piping_stress_loads_to_stru,
        mps_date_structural_modeling_complete,
        mps_structural_engineering_released_to_piping,
        mps_date_structural_engineering_forecast,
        mps_date_structural_engineering_actual,
        mps_date_structural_engineering_complete,
        structural_mps_number,
        mps_foundation_req,
        mps_comment,
        pipecompression,
        piletension,
        tekla_guid,
        tekla_ifcguid,
        tekla_profile,
        tekla_material,
        tekla_warning,
        tekla_version,
        worktype,
        gridno,
        cwarea,
        cwp,
        ewp,
        guid,     
        discipline,
        tagno,
        '' as type1,     -- Missing Column
        units,
        uom,
        material,
        subsystem,
        fireproofing,
        insulation,
        itemno,
        commcode,
        insutype,
        section_name,
        strutype,
        strucate,
        piecemark,
        steeltype,
        site,
        zone,
        cut_length_in,
        nett_volume_in3,
        frmw,
        stru,
        cwarea_of_stru,
        CASE
        WHEN stvval_wor_status_strc_stru_of_stru NOT LIKE 'ME%' THEN ''
        ELSE stvval_wor_status_strc_stru_of_stru
        END AS stvval,
        catalogue_reference,     
        reference_of_the_element,     
        nett_weight_kg as nett_weight,     
        stlweightclass,    
        cast(execution_date as date) as extracted_date,
        source_system_name,
        'VGCP2' project_code,
        REPLACE (ZONE,'/'||REGEXP_SUBSTR(ZONE, '[^/]*$'),'') as Site1,
        REGEXP_SUBSTR(ZONE, '[^/]*$') as MTO_Type1,
        SPLIT_PART(SITE, '-', 2) as WBS, 
        SUBSTRING(ZONE, 2,2) as CWA,
        SUBSTRING(ZONE,2,4) as CWPZONE,
        SPLIT_PART(ZONE,'/',1) as CWP1,
		'VGCP2'|| '_' ||
		Split_part(coalesce(SITE, ''), '-', 2) || '_' ||
		Substring(Replace(ZONE, '/', ''), 2, 2)|| '_' ||
		Substring(Replace(ZONE, '/', ''), 2, 4) AS PROJECT_CODE_WBS_CWA_CWPZONE
    FROM
        {{ source('curated_e3d', 'curated_vg_e3d_vglgensec_sheet') }}
UNION		
SELECT 
        NAME,
        'FLOOR' as type,
        lock,
        owner,
        desc,
        func,
        gtyp,
        numb,
        desp,
        strucl,
        buil,
        shop,
        bang_degree,
        lmirr,
        jois,
        joie,
        jlns,
        jlne,
        jusl,
        ctys,
        ctye,
        srel,
        erel,
        grade,
        matr,
        ispe,
        fire,
        welddp_in,
        posno,
        parnam,
        inre,
        spre,
        anty,
        drgp,
        meml,
        pos,
        ori,
        purp,
        tmrref,
        repcou,
        prodrf,
        bevcod,
        mps_line_number,
        mps_isometric_number,
        mps_date_piping_placeholder_modeling_completed,
        mps_date_piping_stress_loads_to_stru,
        mps_date_structural_modeling_complete,
        mps_structural_engineering_released_to_piping,
        mps_date_structural_engineering_forecast,
        mps_date_structural_engineering_actual,
        mps_date_structural_engineering_complete,
        structural_mps_number,
        mps_foundation_req,
        mps_comment,
        pipecompression,
        piletension,
        tekla_guid,
        tekla_ifcguid,
        tekla_profile,
        tekla_material,
        tekla_warning,
        tekla_version,
        worktype,
        gridno,
        cwarea,
        cwp,
        ewp,
        guid,     
        discipline,
        tagno,
        '' as type1,     -- Missing Column
        units,
        uom,
        material,
        subsystem,
        fireproofing,
        insulation,
        itemno,
        commcode,
        insutype,
        section_name,
        strutype,
        strucate,
        piecemark,
        steeltype,
        site,
        zone,
        cut_length_in,
        nett_volume_in3,
        frmw,
        stru,
        cwarea_of_stru,
        CASE
        WHEN stvval_wor_status_strc_stru_of_stru NOT LIKE 'ME%' THEN ''
        ELSE stvval_wor_status_strc_stru_of_stru
        END AS stvval,
        catalogue_reference,     
        reference_of_the_element,     
        nett_weight_kg as nett_weight,     
        stlweightclass,    
        cast(execution_date as date) as extracted_date,
        source_system_name,
        'VGCP2' project_code,
        REPLACE (ZONE,'/'||REGEXP_SUBSTR(ZONE, '[^/]*$'),'') as Site1,
        REGEXP_SUBSTR(ZONE, '[^/]*$') as MTO_Type1,
        SPLIT_PART(SITE, '-', 2) as WBS, 
        SUBSTRING(ZONE, 2,2) as CWA,
        SUBSTRING(ZONE,2,4) as CWPZONE,
        SPLIT_PART(ZONE,'/',1) as CWP1,
		'VGCP2'|| '_' ||
		Split_part(coalesce(SITE, ''), '-', 2) || '_' ||
		Substring(Replace(ZONE, '/', ''), 2, 2)|| '_' ||
		Substring(Replace(ZONE, '/', ''), 2, 4) AS PROJECT_CODE_WBS_CWA_CWPZONE
    FROM
        {{ source('curated_e3d', 'curated_vg_e3d_vglfloor_sheet') }}
)
