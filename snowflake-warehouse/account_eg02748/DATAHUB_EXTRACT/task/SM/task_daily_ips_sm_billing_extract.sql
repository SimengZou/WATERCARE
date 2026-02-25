CREATE OR REPLACE TASK DATAHUB_EXTRACT.task_DAILY_IPS_SM_BILLING_EXTRACT
  SCHEDULE = 'USING CRON 15 8 * * *  Pacific/Auckland'
  TIMESTAMP_INPUT_FORMAT = 'YYYY-MM-DD HH24'
AS
CALL datahub_extract.sp_daily_ips_sm_billing_extract(1,4);

/* start the task, by default the task is suspended when created */
ALTER TASK datahub_extract.task_daily_ips_sm_billing_extract RESUME;