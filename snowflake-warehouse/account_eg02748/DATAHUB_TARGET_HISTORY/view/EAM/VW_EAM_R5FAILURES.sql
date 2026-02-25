
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5FAILURES
                   as                       
                    SELECT
                        t.FAL_CODE,
                        t.FAL_DESC,
                        t.FAL_GEN,
                        t.FAL_UPDATECOUNT,
                        t.FAL_CREATED,
                        t.FAL_UPDATED,
                        t.FAL_PARTFAILURE,
                        t.FAL_NOTUSED,
                        t.FAL_ENABLEWORKORDERS,
                        t.FAL_GROUP,
                        t.FAL_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5FAILURES as  t
					 union
					        SELECT
                        th.FAL_CODE,
                        th.FAL_DESC,
                        th.FAL_GEN,
                        th.FAL_UPDATECOUNT,
                        th.FAL_CREATED,
                        th.FAL_UPDATED,
                        th.FAL_PARTFAILURE,
                        th.FAL_NOTUSED,
                        th.FAL_ENABLEWORKORDERS,
                        th.FAL_GROUP,
                        th.FAL_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5FAILURES as  th ;
                     