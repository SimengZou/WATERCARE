
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5ERRTEXTS
                   as                       
                    SELECT
                        t.ERT_CODE,
                        t.ERT_TEXT,
                        t.ERT_LANG,
                        t.ERT_TRANSLATE,
                        t.ERT_UPDATECOUNT,
                        t.ERT_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5ERRTEXTS as  t
					 union
					        SELECT
                        th.ERT_CODE,
                        th.ERT_TEXT,
                        th.ERT_LANG,
                        th.ERT_TRANSLATE,
                        th.ERT_UPDATECOUNT,
                        th.ERT_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5ERRTEXTS as  th ;
                     