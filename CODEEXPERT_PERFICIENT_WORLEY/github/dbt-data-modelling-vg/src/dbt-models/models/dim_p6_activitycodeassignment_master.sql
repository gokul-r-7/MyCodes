 {{ config(materialized='table') }}  
   
    select 
    ACA.activityobjectid,
    ACA.activitycodeid,
    AC.codetypename,
    AC.codetypescope,
    AC.codevalue,
    AC.codeconcatname,
    AC.description
    from 
    
    {{ref('dim_p6_activitycodeassignment')}}  ACA
    Left join  {{ref('dim_p6_activitycode')}} AC
    ON AC.objectid = ACA.activitycodeid