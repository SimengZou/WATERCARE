
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5SOAUOMS
                   as                       
                    SELECT
                        t.SUO_CODE,
                        t.SUO_DESC,
                        t.SUO_ACTIVE,
                        t.SUO_UPDATECOUNT,
                        t.SUO_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5SOAUOMS as  t
					 union
					        SELECT
                        th.SUO_CODE,
                        th.SUO_DESC,
                        th.SUO_ACTIVE,
                        th.SUO_UPDATECOUNT,
                        th.SUO_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5SOAUOMS as  th ;
                     