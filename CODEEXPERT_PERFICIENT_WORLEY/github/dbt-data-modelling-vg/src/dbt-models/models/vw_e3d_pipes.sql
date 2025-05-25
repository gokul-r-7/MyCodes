
select pidref
       ,clientdocno
,UNIQUE_LINE_ID
,stvval_wor_status_proc_pipe
,wbs
,cwa
,cwpzone
,workpackno
,progresspercent
,statuslevel
,search_key
,Execution_Date 
from {{ref('fact_e3d_pipes')}}