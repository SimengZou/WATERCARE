
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5HELPTEXTS
                   as                       
                    SELECT
                        t.HEL_FUNCTION,
                        t.HEL_ITEM,
                        t.HEL_TEXT,
                        t.HEL_LANG,
                        t.HEL_POOL,
                        t.HEL_DRILLDOWN,
                        t.HEL_CHANGED,
                        t.HEL_UPDATECOUNT,
                        t.HEL_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5HELPTEXTS as  t
					 union
					        SELECT
                        th.HEL_FUNCTION,
                        th.HEL_ITEM,
                        th.HEL_TEXT,
                        th.HEL_LANG,
                        th.HEL_POOL,
                        th.HEL_DRILLDOWN,
                        th.HEL_CHANGED,
                        th.HEL_UPDATECOUNT,
                        th.HEL_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5HELPTEXTS as  th ;
                     