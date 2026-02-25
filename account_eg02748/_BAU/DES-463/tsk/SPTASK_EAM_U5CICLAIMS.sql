CREATE OR REPLACE TASK DATAHUB_INTEGRATION.TASK_EAM_U5CICLAIMS
                    schedule  = 'USING CRON 0 4 * * * Pacific/Auckland'
    when
    system$stream_has_data('STREAM_EAM_U5CICLAIMS')
  as 
call EAM_TARGET_MERGE_U5CICLAIMS();
alter task DATAHUB_INTEGRATION.TASK_EAM_U5CICLAIMS resume;
