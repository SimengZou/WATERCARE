
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5PREFERENCES
                   as                       
                    SELECT
                        t.PRF_CODE,
                        t.PRF_NR,
                        t.PRF_DEFAULT,
                        t.PRF_UPDATECOUNT,
                        t.PRF_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5PREFERENCES as  t
					 union
					        SELECT
                        th.PRF_CODE,
                        th.PRF_NR,
                        th.PRF_DEFAULT,
                        th.PRF_UPDATECOUNT,
                        th.PRF_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5PREFERENCES as  th ;
                     