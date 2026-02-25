
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5SCHEDULEDJOBSLOCK
                   as                       
                    SELECT
                        t.SJL_CODE,
                        t.SJL_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5SCHEDULEDJOBSLOCK as  t
					 union
					        SELECT
                        th.SJL_CODE,
                        th.SJL_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5SCHEDULEDJOBSLOCK as  th ;
                     