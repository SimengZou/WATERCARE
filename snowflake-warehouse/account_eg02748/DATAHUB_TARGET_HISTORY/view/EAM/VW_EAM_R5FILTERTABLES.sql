
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5FILTERTABLES
                   as                       
                    SELECT
                        t.FTA_TABLE,
                        t.FTA_UPDATECOUNT,
                        t.FTA_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5FILTERTABLES as  t
					 union
					        SELECT
                        th.FTA_TABLE,
                        th.FTA_UPDATECOUNT,
                        th.FTA_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5FILTERTABLES as  th ;
                     