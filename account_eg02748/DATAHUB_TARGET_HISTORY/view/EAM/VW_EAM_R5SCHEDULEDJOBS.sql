
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5SCHEDULEDJOBS
                   as                       
                    SELECT
                        t.SCJ_CODE,
                        t.SCJ_JOBNAME,
                        t.SCJ_SCHCODE,
                        t.SCJ_ACTIVE,
                        t.SCJ_BROKEN,
                        t.SCJ_LASTRUN,
                        t.SCJ_NEXTRUN,
                        t.SCJ_UPDATECOUNT,
                        t.SCJ_SERVERID,
                        t.SCJ_TYPE,
                        t.SCJ_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5SCHEDULEDJOBS as  t
					 union
					        SELECT
                        th.SCJ_CODE,
                        th.SCJ_JOBNAME,
                        th.SCJ_SCHCODE,
                        th.SCJ_ACTIVE,
                        th.SCJ_BROKEN,
                        th.SCJ_LASTRUN,
                        th.SCJ_NEXTRUN,
                        th.SCJ_UPDATECOUNT,
                        th.SCJ_SERVERID,
                        th.SCJ_TYPE,
                        th.SCJ_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5SCHEDULEDJOBS as  th ;
                     