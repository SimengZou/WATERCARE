
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5USERPREFERENCES
                   as                       
                    SELECT
                        t.USP_USER,
                        t.USP_TYPE,
                        t.USP_CODE,
                        t.USP_VALUE,
                        t.USP_UPDATECOUNT,
                        t.USP_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5USERPREFERENCES as  t
					 union
					        SELECT
                        th.USP_USER,
                        th.USP_TYPE,
                        th.USP_CODE,
                        th.USP_VALUE,
                        th.USP_UPDATECOUNT,
                        th.USP_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5USERPREFERENCES as  th ;
                     