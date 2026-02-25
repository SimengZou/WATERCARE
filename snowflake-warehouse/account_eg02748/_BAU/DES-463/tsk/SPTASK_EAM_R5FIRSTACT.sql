CREATE OR REPLACE TASK DATAHUB_INTEGRATION.TASK_EAM_R5FIRSTACT
                    schedule  = 'USING CRON 0 4 * * * Pacific/Auckland'
    when
    system$stream_has_data('STREAM_EAM_R5FIRSTACT')
  as 
call EAM_TARGET_MERGE_R5FIRSTACT();
alter task DATAHUB_INTEGRATION.TASK_EAM_R5FIRSTACT resume;
