
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5EXTENSIBLEFRAMEWORK
                   as                       
                    SELECT
                        t.EFR_NAME,
                        t.EFR_SOURCECODE,
                        t.EFR_ACTIVE,
                        t.EFR_USERFUNCTION,
                        t.EFR_UPDATECOUNT,
                        t.EFR_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5EXTENSIBLEFRAMEWORK as  t
					 union
					        SELECT
                        th.EFR_NAME,
                        th.EFR_SOURCECODE,
                        th.EFR_ACTIVE,
                        th.EFR_USERFUNCTION,
                        th.EFR_UPDATECOUNT,
                        th.EFR_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5EXTENSIBLEFRAMEWORK as  th ;
                     