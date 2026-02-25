
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5SCREENCACHE
                   as                       
                    SELECT
                        t.SNC_USER,
                        t.SNC_FUNCTION,
                        t.SNC_UPDATECOUNT,
                        t.SNC_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5SCREENCACHE as  t
					 union
					        SELECT
                        th.SNC_USER,
                        th.SNC_FUNCTION,
                        th.SNC_UPDATECOUNT,
                        th.SNC_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5SCREENCACHE as  th ;
                     