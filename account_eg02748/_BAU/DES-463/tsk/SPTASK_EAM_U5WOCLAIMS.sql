CREATE OR REPLACE TASK DATAHUB_INTEGRATION.TASK_EAM_U5WOCLAIMS
                    schedule  = 'USING CRON 0 4 * * * Pacific/Auckland'
    when
    system$stream_has_data('STREAM_EAM_U5WOCLAIMS')
  as 
call EAM_TARGET_MERGE_U5WOCLAIMS();
alter task DATAHUB_INTEGRATION.TASK_EAM_U5WOCLAIMS resume;
