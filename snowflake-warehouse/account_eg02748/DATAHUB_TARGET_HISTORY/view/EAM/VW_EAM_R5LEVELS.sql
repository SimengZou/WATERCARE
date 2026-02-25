
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5LEVELS
                   as                       
                    SELECT
                        t.LVL_LEVEL,
                        t.LVL_UPDATECOUNT,
                        t.LVL_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5LEVELS as  t
					 union
					        SELECT
                        th.LVL_LEVEL,
                        th.LVL_UPDATECOUNT,
                        th.LVL_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5LEVELS as  th ;
                     