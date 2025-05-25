{%- set run_date = "CURRENT_TIMESTAMP()" -%}
{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_spel_t_cable/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}

SELECT
C.sp_id,
CAST(C.restartflag AS DECIMAL(38,0)) AS  restartflag,
CAST(C.noconductor AS DECIMAL(38,0)) AS  noconductor,
CAST(C.noofsets AS DECIMAL(38,0)) AS  noofsets,
CAST(C.noofconductingroup AS DECIMAL(38,0)) AS  noofconductingroup,
CAST(C.routing AS DECIMAL(38,0)) AS  routing,
C.routingpath,
CAST(C.cablesizingflag AS DECIMAL(38,0)) AS  cablesizingflag,
C.bendingradius,
CAST(C.sp_bendingradiussi AS float) AS  sp_bendingradiussi,
C.pulltension,
CAST(C.sp_pulltensionsi AS float) AS sp_pulltensionsi,
CAST(C.recommendconductorssize AS DECIMAL(38,0)) AS  recommendconductorssize,
CAST(C.cableusage AS DECIMAL(38,0)) AS  cableusage,
CAST(C.cablecategory AS DECIMAL(38,0)) AS  cablecategory,
CAST(C.cablespecification AS DECIMAL(38,0)) AS  cablespecification,
CAST(C.armortype AS DECIMAL(38,0)) AS  armortype,
CAST(C.jacketinsulationmaterial AS DECIMAL(38,0)) AS  jacketinsulationmaterial,
CAST(C.jacketcolor AS DECIMAL(38,0)) AS  jacketcolor,
CAST(C.jacketinsulationtemprating AS DECIMAL(38,0)) AS  jacketinsulationtemprating,
CAST(C.innerinsulationtemprating AS DECIMAL(38,0)) AS  innerinsulationtemprating,
CAST(C.noofparalleledcable AS DECIMAL(38,0)) AS  noofparalleledcable,
CAST(C.calcvoltagedroppercent AS float) AS calcvoltagedroppercent,
C.designlength,
CAST(C.sp_designlengthsi AS float) AS sp_designlengthsi,
C.deratedampacity,
C.weightperlength,
CAST(C.sp_weightperlengthsi AS float) AS sp_weightperlengthsi,
CAST(C.conductorarrangement AS DECIMAL(38,0)) AS  conductorarrangement,
C.actuallength,
CAST(C.sp_actuallengthsi AS float) AS sp_actuallengthsi,
C.basicampacityinground,
CAST(C.sp_basicampacityingroundsi AS float) AS sp_basicampacityingroundsi,
C.basicampacityinair,
CAST(C.sp_basicampacityinairsi AS float) AS sp_basicampacityinairsi,
CAST(C.calcstartingvoltagedrop AS float) AS calcstartingvoltagedrop,
CAST(C.noadditionalconductor AS DECIMAL(38,0)) AS  noadditionalconductor,
CAST(C.additionalconductorsize AS DECIMAL(38,0)) AS  additionalconductorsize,
CAST(C.ratedvoltage AS DECIMAL(38,0)) AS  ratedvoltage,
CAST(C.customderatingfactor AS float) AS customderatingfactor,
CAST(C.customutilizationfactor AS float) AS customutilizationfactor,
C.sp_connectionside2id,
C.sp_trayid,
CAST(C.powerfactoratstarting AS float) AS powerfactoratstarting,
CAST(C.loadchangingflag AS DECIMAL(38,0)) AS  loadchangingflag,
C.sp_connectionside1id,
CAST(C.sp_deratedampacitysi AS float) AS sp_deratedampacitysi,
C.sp_refcolorpatternid,
C.estimatedcablelength,
CAST(C.sp_estimatedcablelengthsi AS float) AS sp_estimatedcablelengthsi,
CAST(C.allowvoltdroppercentrunning AS float) AS allowvoltdroppercentrunning,
CAST(C.allowvoltdroppercentstart AS float) AS allowvoltdroppercentstart,
CAST(C.calcnoofparalleledcable AS DECIMAL(38,0)) AS  calcnoofparalleledcable,
C.sp_calcrefcableid,
CAST(C.systemcablecategory AS DECIMAL(38,0)) AS  systemcablecategory,
C.cableformation,
C.length,
CAST(C.sp_lengthsi AS float) AS sp_lengthsi,
C.startingcurrent,
CAST(C.sp_startingcurrentsi AS float) AS sp_startingcurrentsi,
CAST(C.powerfactorfullload AS float) AS powerfactorfullload,
C.systemnote,
C.lastcalcmaxcablelength,
CAST(C.sp_lastcalcmaxcablelengthsi AS float) AS sp_lastcalcmaxcablelengthsi,
CAST(C.cablesettype AS DECIMAL(38,0)) AS  cablesettype,
C.shortcircuitduration,
CAST(C.sp_shortcircuitdurationsi AS float) AS sp_shortcircuitdurationsi,
C.shortcircuitfaultcurrent,
CAST(C.sp_shortcircuitfaultcurrentsi AS float) AS sp_shortcircuitfaultcurrentsi,
CAST(C.shortcircuitminimumarea AS DECIMAL(38,0)) AS  shortcircuitminimumarea,
CAST(C.overallshieldflag AS DECIMAL(38,0)) AS  overallshieldflag,
CAST(C.communicationwireflag AS DECIMAL(38,0)) AS  communicationwireflag,
CAST(C.overallshieldsize AS DECIMAL(38,0)) AS  overallshieldsize,
CAST(C.overallshieldmaterial AS DECIMAL(38,0)) AS  overallshieldmaterial,
CAST(C.enableshortcircuitsizing AS DECIMAL(38,0)) AS  enableshortcircuitsizing,
C.sp_cabledrumid,
CAST(C.pullingarea AS DECIMAL(38,0)) AS  pullingarea,
C.tail1length,
CAST(C.sp_tail1lengthsi AS float) AS sp_tail1lengthsi,
C.tail2length,
CAST(C.sp_tail2lengthsi AS float) AS sp_tail2lengthsi,
C.sparelength,
CAST(C.sp_sparelengthsi AS float) AS sp_sparelengthsi,
CAST(C.apply25percentfactorflag AS DECIMAL(38,0)) AS  apply25percentfactorflag,
CAST(C.ambienttemperaturefactor AS float) AS ambienttemperaturefactor,
CAST(C.neutralearthing AS DECIMAL(38,0)) AS  neutralearthing,
C.parallelcablesuffix,
C.sp_refglandside1id,
C.sp_refglandside2id,
CAST(C.protectiondevicesettingflag AS DECIMAL(38,0)) AS  protectiondevicesettingflag,
C.protectiondevicesetting,
CAST(C.sp_protectiondevicesettingsi AS float) AS sp_protectiondevicesettingsi,
C.routedlength,
CAST(C.sp_routedlengthsi AS float) AS sp_routedlengthsi,
C.armorresistivity,
CAST(C.sp_armorresistivitysi AS float) AS sp_armorresistivitysi,
C.armorreactance,
CAST(C.sp_armorreactancesi AS float) AS sp_armorreactancesi,
C.externalearthimpedance,
CAST(C.sp_externalearthimpedancesi AS float) AS sp_externalearthimpedancesi,
CAST(C.grandingforimpedancecalc AS DECIMAL(38,0)) AS  grandingforimpedancecalc,
C.calcearthloopimpedance,
CAST(C.sp_calcearthloopimpedancesi AS float) AS sp_calcearthloopimpedancesi,
CAST(C.earthloopimpedancecalcrequired AS DECIMAL(38,0)) AS  earthloopimpedancecalcrequired,
CAST(C.routingstatus AS DECIMAL(38,0)) AS  routingstatus,
CAST(C.segregationlevel AS DECIMAL(38,0)) AS  segregationlevel,
C.basicampacityinduct,
CAST(C.sp_basicampacityinductsi AS float) AS sp_basicampacityinductsi,
CAST(C.coresscreenmaterial AS DECIMAL(38,0)) AS  coresscreenmaterial,
CAST(C.cablescreenmaterial AS DECIMAL(38,0)) AS  cablescreenmaterial,
C.sp_externalroutinglength,
CAST(C.sp_useexternalroutinglength AS DECIMAL(38,0)) AS  sp_useexternalroutinglength,
CAST(C.sp_externalroutinglengthsi AS float) AS sp_externalroutinglengthsi,
CAST(C.sp_codefactor AS float) AS sp_codefactor,
CAST(C.sp_numberofsplice AS DECIMAL(38,0)) AS  sp_numberofsplice,
CAST(C.sp_kkscableapplicationarea AS DECIMAL(38,0)) AS  sp_kkscableapplicationarea,
C.sp_actualroutingpath,
C.sp_externalroutingpath,
C.sp_cableassemblyid,
C.sp_resistivityzerosequence,
CAST(C.sp_resistivityzerosequencesi AS float) AS sp_resistivityzerosequencesi,
C.sp_reactancezerosequence,
CAST(C.sp_reactancezerosequencesi AS float) AS sp_reactancezerosequencesi,
C.sp_cableaccess,
CAST(C.sp_corephaseusage AS DECIMAL(38,0)) AS  sp_corephaseusage,
CAST(C.sp_phaselabel AS DECIMAL(38,0)) AS  sp_phaselabel,
CAST(C.sp_locktodrum AS DECIMAL(38,0)) AS  sp_locktodrum,
C.sp_installedlength,
CAST(C.sp_sp_installedlengthsi AS float) AS sp_sp_installedlengthsi,
C.sp_cablemarkend1,
CAST(C.sp_sp_cablemarkend1si AS float) AS sp_sp_cablemarkend1si,
C.sp_cablemarkend2,
CAST(C.sp_sp_cablemarkend2si AS float) AS sp_sp_cablemarkend2si,
CAST(C.sp_isinstalled AS DECIMAL(38,0)) AS  sp_isinstalled,
C.sp_custombasicampacity,
CAST(C.sp_sp_custombasicampacitysi AS float) AS sp_sp_custombasicampacitysi,
CAST(C.sp_toberoutedins3d AS DECIMAL(38,0)) AS  sp_toberoutedins3d,
CAST(C.sp_voltagedropfullload AS float) AS sp_voltagedropfullload,
CAST(C.sp_voltagedropstarting AS float) AS sp_voltagedropstarting,
CAST(C.sp_noadditionalgndconductor AS DECIMAL(38,0)) AS  sp_noadditionalgndconductor,
CAST(C.sp_additionalgndconductorsize AS DECIMAL(38,0)) AS  sp_additionalgndconductorsize,
CAST(C.sp_retaincablecuts AS DECIMAL(38,0)) AS  sp_retaincablecuts,
C.vg_installationtype,
C.vg_schem_wiringno,
C.vg_conduitlength,
C.vg_conduitsize,
C.vg_plandrawing,
codelist_text as WBS, 
vg_cwpzones_elecequip as CWPZONE,
SUBSTRING(vg_cwpzones_elecequip, 1,2) as CWA,
itemtag as CABLE_TAG,
description as CABLE_DESCRIPTION,
Case when cablecategory = 1 then 'Power'
     when cablecategory = 2 then 'Instrumentation'
     when cablecategory = 3 then 'Control'
     when cablecategory = 4 then 'Grounding' end as CABLE_TYPE,
sp_connectionside1id as FROM,
sp_connectionside2id as TO,
CAST(TRIM(BOTH ' ' FROM REPLACE(designlength, ' ft', '')) AS DECIMAL(38,0)) AS DESIGN_LENGTH,
CAST(TRIM(BOTH ' ' FROM REPLACE(sp_actuallengthsi, ' m', '')) AS DECIMAL(38,0)) * 3.28084 AS ACTUAL_LENGTH,
    COALESCE(codelist_text, '') || ' ' || 
    COALESCE(vg_cwpzones_elecequip, '') || ' ' || 
    COALESCE(SUBSTRING(vg_cwpzones_elecequip, 1, 2), '') || ' ' || 
    COALESCE(itemtag, '') || ' ' || 
    COALESCE(description, '') || ' ' || 
    COALESCE(
        CASE 
            WHEN cablecategory = 1 THEN 'Power'
            WHEN cablecategory = 2 THEN 'Instrumentation'
            WHEN cablecategory = 3 THEN 'Control'
            WHEN cablecategory = 4 THEN 'Grounding'
        END, 
        '') || ' ' || 
    COALESCE(sp_connectionside1id, '') || ' ' || 
    COALESCE(sp_connectionside2id, '') || ' ' || 
    COALESCE(CAST(TRIM(BOTH ' ' FROM REPLACE(DESIGNLENGTH, ' ft', '')) AS DECIMAL), 0) || ' ' || 
    COALESCE(CAST(TRIM(BOTH ' ' FROM REPLACE(sp_actuallengthsi, ' m', '')) AS DECIMAL) * 3.28084, 0) 
AS SEARCH_KEY,
source_system_name,
'VGCP2' project_code,
cast(C.execution_date as date) as extracted_date,
{{run_date}} as model_created_date,
{{run_date}} as model_updated_date,
{{ generate_load_id(model) }} as model_load_id
from   {{ source('curated_spel', 'curated_t_cable') }} C
LEFT OUTER JOIN (Select distinct sp_id ,codelist_text , Q.execution_date From 
{{ source('curated_spel', 'curated_t_equipment') }} Q
LEFT OUTER JOIN (Select distinct codelist_text, codelist_index, execution_date From   {{ source('curated_spel', 'curated_codelists') }}
where execution_date = (Select max(execution_date) From {{ source('curated_spel', 'curated_codelists') }} )
and codelist_number = 12001)L
on CODELIST_INDEX = VG_AREANAME
and Q.execution_date = (Select max(execution_date) From {{ source('curated_spel', 'curated_t_equipment') }})
and Q.execution_date = L.execution_date
) Q
on C.sp_id = Q.sp_id  
and C.execution_date = Q.execution_date
LEFT OUTER JOIN (Select distinct sp_id , vg_cwpzones_elecequip ,execution_date From {{ source('curated_spel', 'curated_t_electricalequipment') }} where execution_date = (Select max(execution_date) From {{ source('curated_spel', 'curated_t_electricalequipment') }})) E
ON C.sp_id = E.sp_id
and C.execution_date = E.execution_date
LEFT OUTER JOIN (Select distinct sp_id , itemtag , execution_date From {{ source('curated_spel', 'curated_t_plantitem') }}
where execution_date = (Select max(execution_date) From {{ source('curated_spel', 'curated_t_plantitem') }})) P
on C.sp_id = P.sp_id
LEFT OUTER JOIN (Select distinct sp_id ,execution_date, description From  {{ source('curated_spel', 'curated_t_modelitem') }}
where execution_date = (Select max(execution_date) From {{ source('curated_spel', 'curated_t_modelitem') }})) M
on C.sp_id = M.sp_id
and C.execution_date = M.execution_date

