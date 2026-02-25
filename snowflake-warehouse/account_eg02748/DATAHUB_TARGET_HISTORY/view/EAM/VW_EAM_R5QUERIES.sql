
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5QUERIES
                   as                       
                    SELECT
                        t.QUE_CODE,
                        t.QUE_TEXT,
                        t.QUE_NORMAL,
                        t.QUE_ASSET,
                        t.QUE_INBOX,
                        t.QUE_KPI,
                        t.QUE_LOOKUP,
                        t.QUE_UPDATECOUNT,
                        t.QUE_UPDATED,
                        t.QUE_CHART,
                        t.QUE_DESC,
                        t.QUE_NOTE,
                        t.QUE_EQUIPMENTRANKING,
                        t.QUE_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5QUERIES as  t
					 union
					        SELECT
                        th.QUE_CODE,
                        th.QUE_TEXT,
                        th.QUE_NORMAL,
                        th.QUE_ASSET,
                        th.QUE_INBOX,
                        th.QUE_KPI,
                        th.QUE_LOOKUP,
                        th.QUE_UPDATECOUNT,
                        th.QUE_UPDATED,
                        th.QUE_CHART,
                        th.QUE_DESC,
                        th.QUE_NOTE,
                        th.QUE_EQUIPMENTRANKING,
                        th.QUE_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5QUERIES as  th ;
                     