
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5DOCSIZES
                   as                       
                    SELECT
                        t.DSZ_CODE,
                        t.DSZ_DESC,
                        t.DSZ_UPDATECOUNT,
                        t.DSZ_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5DOCSIZES as  t
					 union
					        SELECT
                        th.DSZ_CODE,
                        th.DSZ_DESC,
                        th.DSZ_UPDATECOUNT,
                        th.DSZ_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5DOCSIZES as  th ;
                     