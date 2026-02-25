
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5ORGTABCOLUMNS
                   as                       
                    SELECT
                        t.OTC_TABLE,
                        t.OTC_COLUMN,
                        t.OTC_UPDATECOUNT,
                        t.OTC_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5ORGTABCOLUMNS as  t
					 union
					        SELECT
                        th.OTC_TABLE,
                        th.OTC_COLUMN,
                        th.OTC_UPDATECOUNT,
                        th.OTC_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5ORGTABCOLUMNS as  th ;
                     