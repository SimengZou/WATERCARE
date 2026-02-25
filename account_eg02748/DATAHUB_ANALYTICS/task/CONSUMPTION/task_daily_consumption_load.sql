CREATE OR REPLACE TASK DATAHUB_ANALYTICS.task_DAILY_CONSUMPTION_LOAD
  WAREHOUSE = DEV_WH_LOADING
  SCHEDULE = 'USING CRON 20 9 * * *  Pacific/Auckland'
  TIMESTAMP_INPUT_FORMAT = 'YYYY-MM-DD HH24'
AS
CALL DATAHUB_ANALYTICS.sp_daily_consumption_load();

/* start the task, by default the task is suspended when created */
ALTER TASK datahub_analytics.task_daily_consumption_load RESUME;