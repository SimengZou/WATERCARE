
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5CONCLAUSES
                   as                       
                    SELECT
                        t.CCL_CODE,
                        t.CCL_DESC,
                        t.CCL_CLASS,
                        t.CCL_PARENT,
                        t.CCL_LINE,
                        t.CCL_SYSTEM,
                        t.CCL_ORG,
                        t.CCL_CLASS_ORG,
                        t.CCL_UPDATECOUNT,
                        t.CCL_NOTUSED,
                        t.CCL_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5CONCLAUSES as  t
					 union
					        SELECT
                        th.CCL_CODE,
                        th.CCL_DESC,
                        th.CCL_CLASS,
                        th.CCL_PARENT,
                        th.CCL_LINE,
                        th.CCL_SYSTEM,
                        th.CCL_ORG,
                        th.CCL_CLASS_ORG,
                        th.CCL_UPDATECOUNT,
                        th.CCL_NOTUSED,
                        th.CCL_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5CONCLAUSES as  th ;
                     