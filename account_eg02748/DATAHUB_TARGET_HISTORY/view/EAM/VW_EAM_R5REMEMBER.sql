
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5REMEMBER
                   as                       
                    SELECT
                        t.RMB_FUNCTION,
                        t.RMB_ITEM,
                        t.RMB_ITEM2,
                        t.RMB_RENTITY,
                        t.RMB_UPDATECOUNT,
                        t.RMB_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5REMEMBER as  t
					 union
					        SELECT
                        th.RMB_FUNCTION,
                        th.RMB_ITEM,
                        th.RMB_ITEM2,
                        th.RMB_RENTITY,
                        th.RMB_UPDATECOUNT,
                        th.RMB_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5REMEMBER as  th ;
                     