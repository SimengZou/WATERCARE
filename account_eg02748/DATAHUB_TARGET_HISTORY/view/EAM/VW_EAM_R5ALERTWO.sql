
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5ALERTWO
                   as                       
                    SELECT
                        t.ALW_ALERT,
                        t.ALW_DELAY,
                        t.ALW_DELAYUOM,
                        t.ALW_STANDWORK,
                        t.ALW_WORKORDERORG,
                        t.ALW_OBJECTFIELDID,
                        t.ALW_OBJECTORGFIELDID,
                        t.ALW_WORKORDERORGFIELDID,
                        t.ALW_PROBLEMCODEFIELDID,
                        t.ALW_JOBTYPEFIELDID,
                        t.ALW_PRIORITYFIELDID,
                        t.ALW_DURATIONFIELDID,
                        t.ALW_SCHEDSTARTFIELDID,
                        t.ALW_REQUESTSTARTFIELDID,
                        t.ALW_REQUESTENDFIELDID,
                        t.ALW_WODESC,
                        t.ALW_WOCOMMENT,
                        t.ALW_UPDATECOUNT,
                        t.ALW_INCLUDENONCONFORMITIES,
                        t.ALW_DUENONCONFORMITIESONLY,
                        t.ALW_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5ALERTWO as  t
					 union
					        SELECT
                        th.ALW_ALERT,
                        th.ALW_DELAY,
                        th.ALW_DELAYUOM,
                        th.ALW_STANDWORK,
                        th.ALW_WORKORDERORG,
                        th.ALW_OBJECTFIELDID,
                        th.ALW_OBJECTORGFIELDID,
                        th.ALW_WORKORDERORGFIELDID,
                        th.ALW_PROBLEMCODEFIELDID,
                        th.ALW_JOBTYPEFIELDID,
                        th.ALW_PRIORITYFIELDID,
                        th.ALW_DURATIONFIELDID,
                        th.ALW_SCHEDSTARTFIELDID,
                        th.ALW_REQUESTSTARTFIELDID,
                        th.ALW_REQUESTENDFIELDID,
                        th.ALW_WODESC,
                        th.ALW_WOCOMMENT,
                        th.ALW_UPDATECOUNT,
                        th.ALW_INCLUDENONCONFORMITIES,
                        th.ALW_DUENONCONFORMITIESONLY,
                        th.ALW_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5ALERTWO as  th ;
                     