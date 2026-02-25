
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5CAUSES
                   as                       
                    SELECT
                        t.CAU_CODE,
                        t.CAU_DESC,
                        t.CAU_GEN,
                        t.CAU_UPDATECOUNT,
                        t.CAU_CREATED,
                        t.CAU_UPDATED,
                        t.CAU_PARTFAILURE,
                        t.CAU_NOTUSED,
                        t.CAU_ENABLEWORKORDERS,
                        t.CAU_GROUP,
                        t.CAU_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5CAUSES as  t
					 union
					        SELECT
                        th.CAU_CODE,
                        th.CAU_DESC,
                        th.CAU_GEN,
                        th.CAU_UPDATECOUNT,
                        th.CAU_CREATED,
                        th.CAU_UPDATED,
                        th.CAU_PARTFAILURE,
                        th.CAU_NOTUSED,
                        th.CAU_ENABLEWORKORDERS,
                        th.CAU_GROUP,
                        th.CAU_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5CAUSES as  th ;
                     