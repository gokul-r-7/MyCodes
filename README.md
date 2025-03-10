GLUE JOB COST CALCULATION

No. of DPU * Run time * 0.44

First time ran the job with 20 DPUs , it got failed in 6 mins due to low configuration
- 20 DPUs
- The job ran for 6 minutes (0.1 hours, since 6 minutes is 1/10 of an hour).
- Cost for this run = 20 DPUs × 0.1 hours × $0.44 = $0.88

Second time ran the job with 40 DPUs , it got failed 10 mins due to low configuration
 - 40 DPUs
 - The job ran for 10 minutes (0.167 hours, since 10 minutes is 10/60 of an hour).
 - Cost for this run = 40 DPUs × 0.167 hours × $0.44 = $2.93

Third time ran the job with 80 DPUs , it succeeded in 1hr 6 mins
- 80 DPUs
- The job ran for 1 hour and 6 minutes (1.1 hours).
- Cost for this run = 80 DPUs × 1.1 hours × $0.44 = $38.72. 

Total Cost as per calculation - $0.88 + $2.93 + $38.72  = $42.53

Cost in AWS Cost Explorer : $41.64
