
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5WSDUP2
                   as                       
                    SELECT
                        t.WD2_CODE,
                        t.WD2_DESC,
                        t.WD2_TIME,
                        t.WD2_TYPE,
                        t.WD2_UPDATECOUNT,
                        t.WD2_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5WSDUP2 as  t
					 union
					        SELECT
                        th.WD2_CODE,
                        th.WD2_DESC,
                        th.WD2_TIME,
                        th.WD2_TYPE,
                        th.WD2_UPDATECOUNT,
                        th.WD2_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5WSDUP2 as  th ;
                     