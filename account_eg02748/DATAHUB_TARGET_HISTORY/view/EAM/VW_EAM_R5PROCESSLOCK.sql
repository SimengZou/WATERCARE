
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5PROCESSLOCK
                   as                       
                    SELECT
                        t.PLK_CODE,
                        t.PLK_DESC,
                        t.PLK_LOCKTIME,
                        t.PLK_UPDATECOUNT,
                        t.PLK_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5PROCESSLOCK as  t
					 union
					        SELECT
                        th.PLK_CODE,
                        th.PLK_DESC,
                        th.PLK_LOCKTIME,
                        th.PLK_UPDATECOUNT,
                        th.PLK_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5PROCESSLOCK as  th ;
                     