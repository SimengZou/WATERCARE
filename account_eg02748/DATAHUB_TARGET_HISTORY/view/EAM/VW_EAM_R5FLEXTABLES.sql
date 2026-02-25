
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5FLEXTABLES
                   as                       
                    SELECT
                        t.FLT_TABLE,
                        t.FLT_UPDATECOUNT,
                        t.FLT_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5FLEXTABLES as  t
					 union
					        SELECT
                        th.FLT_TABLE,
                        th.FLT_UPDATECOUNT,
                        th.FLT_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5FLEXTABLES as  th ;
                     