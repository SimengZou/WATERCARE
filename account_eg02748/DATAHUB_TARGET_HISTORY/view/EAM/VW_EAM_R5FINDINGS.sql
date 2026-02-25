
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5FINDINGS
                   as                       
                    SELECT
                        t.FND_CODE,
                        t.FND_DESC,
                        t.FND_CLASS,
                        t.FND_GEN,
                        t.FND_CLASS_ORG,
                        t.FND_UPDATECOUNT,
                        t.FND_CREATED,
                        t.FND_UPDATED,
                        t.FND_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5FINDINGS as  t
					 union
					        SELECT
                        th.FND_CODE,
                        th.FND_DESC,
                        th.FND_CLASS,
                        th.FND_GEN,
                        th.FND_CLASS_ORG,
                        th.FND_UPDATECOUNT,
                        th.FND_CREATED,
                        th.FND_UPDATED,
                        th.FND_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5FINDINGS as  th ;
                     