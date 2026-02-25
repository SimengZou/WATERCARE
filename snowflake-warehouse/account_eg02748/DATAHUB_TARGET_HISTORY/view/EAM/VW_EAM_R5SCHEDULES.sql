
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5SCHEDULES
                   as                       
                    SELECT
                        t.SCH_CODE,
                        t.SCH_NAME,
                        t.SCH_DESCRIPTION,
                        t.SCH_TYPE,
                        t.SCH_MONTH,
                        t.SCH_DAYOFMONTH,
                        t.SCH_DAYOFWEEK,
                        t.SCH_HOUR,
                        t.SCH_MINUTE,
                        t.SCH_UPDATECOUNT,
                        t.SCH_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5SCHEDULES as  t
					 union
					        SELECT
                        th.SCH_CODE,
                        th.SCH_NAME,
                        th.SCH_DESCRIPTION,
                        th.SCH_TYPE,
                        th.SCH_MONTH,
                        th.SCH_DAYOFMONTH,
                        th.SCH_DAYOFWEEK,
                        th.SCH_HOUR,
                        th.SCH_MINUTE,
                        th.SCH_UPDATECOUNT,
                        th.SCH_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5SCHEDULES as  th ;
                     