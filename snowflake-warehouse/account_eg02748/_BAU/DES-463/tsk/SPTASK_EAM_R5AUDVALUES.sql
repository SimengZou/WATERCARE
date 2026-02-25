CREATE OR REPLACE TASK DATAHUB_INTEGRATION.TASK_EAM_R5AUDVALUES
                    schedule  = 'USING CRON 0 4 * * * Pacific/Auckland'
    when
    system$stream_has_data('STREAM_EAM_R5AUDVALUES')
  as 
call EAM_TARGET_MERGE_R5AUDVALUES();
alter task DATAHUB_INTEGRATION.TASK_EAM_R5AUDVALUES resume;
