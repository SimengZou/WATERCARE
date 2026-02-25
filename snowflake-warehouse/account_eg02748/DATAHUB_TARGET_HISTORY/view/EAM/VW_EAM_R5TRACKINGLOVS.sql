
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5TRACKINGLOVS
                   as                       
                    SELECT
                        t.TKV_CODE,
                        t.TKV_SQL,
                        t.TKV_UPDATED,
                        t.TKV_UPDATECOUNT,
                        t.TKV_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5TRACKINGLOVS as  t
					 union
					        SELECT
                        th.TKV_CODE,
                        th.TKV_SQL,
                        th.TKV_UPDATED,
                        th.TKV_UPDATECOUNT,
                        th.TKV_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5TRACKINGLOVS as  th ;
                     