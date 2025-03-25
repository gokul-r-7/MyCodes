Existing Adobe Framework has Config tables, Audit tables to read Process IDs & adding entries of job status
Existing Adobe Framework run completely based on scheduled time given in cloud watch
Existing  Adobe Framework has used 2 lambda functions & 2 glue jobs to read the adobe API in one job, transform and process it in another job
Crawlers has been used in Existing Adobe framework to create tables.
SNS has been used to send the failure notifications to respective team in Existing Adobe Framework
Secret Managers has been used to store the API endpoints & credentials in Existing Adobe Framework
Two jobs has been created Historical load & Incremental Load in New Adobe Framework
Historical load job run based on ON-Demand Trigger, Incremental Load job run based on Scheduled basis
Config file has been used to store & pass the parameters while calling Adobe API
Historical load & Incremental load date information has been added in the config file
Without using crawler, the Athena table has been created by using query in the job itself
![Uploading image.png…]()
