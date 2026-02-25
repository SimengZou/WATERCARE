
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5ERRSOURCE
                   as                       
                    SELECT
                        t.ERS_SOURCE,
                        t.ERS_TYPE,
                        t.ERS_DESC,
                        t.ERS_NUMBER,
                        t.ERS_CODE,
                        t.ERS_NAME,
                        t.ERS_UPDATECOUNT,
                        t.ERS_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5ERRSOURCE as  t
					 union
					        SELECT
                        th.ERS_SOURCE,
                        th.ERS_TYPE,
                        th.ERS_DESC,
                        th.ERS_NUMBER,
                        th.ERS_CODE,
                        th.ERS_NAME,
                        th.ERS_UPDATECOUNT,
                        th.ERS_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5ERRSOURCE as  th ;
                     