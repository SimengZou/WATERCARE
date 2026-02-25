
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5ACTSCHEDULES
                   as                       
                    SELECT
                        t.ACS_EVENT,
                        t.ACS_ACTIVITY,
                        t.ACS_COMMENT,
                        t.ACS_MRC,
                        t.ACS_TRADE,
                        t.ACS_OBJECT,
                        t.ACS_SCHED,
                        t.ACS_STARTTIME,
                        t.ACS_ENDTIME,
                        t.ACS_HOURS,
                        t.ACS_RESPONSIBLE,
                        t.ACS_SHIFT,
                        t.ACS_SCHEDULER,
                        t.ACS_FROZEN,
                        t.ACS_OBJECT_ORG,
                        t.ACS_MPPROJ,
                        t.ACS_UPDATECOUNT,
                        t.ACS_CODE,
                        t.ACS_UPDATED,
                        t.ACS_SHIFTSCHEDULESESSION,
                        t.ACS_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5ACTSCHEDULES as  t
					 union
					        SELECT
                        th.ACS_EVENT,
                        th.ACS_ACTIVITY,
                        th.ACS_COMMENT,
                        th.ACS_MRC,
                        th.ACS_TRADE,
                        th.ACS_OBJECT,
                        th.ACS_SCHED,
                        th.ACS_STARTTIME,
                        th.ACS_ENDTIME,
                        th.ACS_HOURS,
                        th.ACS_RESPONSIBLE,
                        th.ACS_SHIFT,
                        th.ACS_SCHEDULER,
                        th.ACS_FROZEN,
                        th.ACS_OBJECT_ORG,
                        th.ACS_MPPROJ,
                        th.ACS_UPDATECOUNT,
                        th.ACS_CODE,
                        th.ACS_UPDATED,
                        th.ACS_SHIFTSCHEDULESESSION,
                        th.ACS_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5ACTSCHEDULES as  th ;
                     