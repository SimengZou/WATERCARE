CREATE OR REPLACE TASK DATAHUB_INTEGRATION.TASK_EAM_R5JOBPARAM
                    schedule  = 'USING CRON 0 4 * * * Pacific/Auckland'
    when
    system$stream_has_data('STREAM_EAM_R5JOBPARAM')
  as 
call EAM_TARGET_MERGE_R5JOBPARAM();
alter task DATAHUB_INTEGRATION.TASK_EAM_R5JOBPARAM resume;
