
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5WSPROMPTS
                   as                       
                    SELECT
                        t.WST_CODE,
                        t.WST_DESC,
                        t.WST_NOTUSED,
                        t.WST_UPDATECOUNT,
                        t.WST_UPDATED,
                        t.WST_FUNCTION,
                        t.WST_TAB,
                        t.WST_SYSTEM,
                        t.WST_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5WSPROMPTS as  t
					 union
					        SELECT
                        th.WST_CODE,
                        th.WST_DESC,
                        th.WST_NOTUSED,
                        th.WST_UPDATECOUNT,
                        th.WST_UPDATED,
                        th.WST_FUNCTION,
                        th.WST_TAB,
                        th.WST_SYSTEM,
                        th.WST_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5WSPROMPTS as  th ;
                     