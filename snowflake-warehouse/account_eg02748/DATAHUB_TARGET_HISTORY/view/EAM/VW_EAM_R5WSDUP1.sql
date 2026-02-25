
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5WSDUP1
                   as                       
                    SELECT
                        t.WD1_CODE,
                        t.WD1_DESC,
                        t.WD1_TIME,
                        t.WD1_TYPE,
                        t.WD1_UPDATECOUNT,
                        t.WD1_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5WSDUP1 as  t
					 union
					        SELECT
                        th.WD1_CODE,
                        th.WD1_DESC,
                        th.WD1_TIME,
                        th.WD1_TYPE,
                        th.WD1_UPDATECOUNT,
                        th.WD1_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5WSDUP1 as  th ;
                     