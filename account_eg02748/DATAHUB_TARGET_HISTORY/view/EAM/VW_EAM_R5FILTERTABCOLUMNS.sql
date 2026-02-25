
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5FILTERTABCOLUMNS
                   as                       
                    SELECT
                        t.FTC_TABLE,
                        t.FTC_COLUMN,
                        t.FTC_DATATYPE,
                        t.FTC_UPDATECOUNT,
                        t.FTC_LASTSAVED,
                        t.FTC_CLOB,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5FILTERTABCOLUMNS as  t
					 union
					        SELECT
                        th.FTC_TABLE,
                        th.FTC_COLUMN,
                        th.FTC_DATATYPE,
                        th.FTC_UPDATECOUNT,
                        th.FTC_LASTSAVED,
                        th.FTC_CLOB,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5FILTERTABCOLUMNS as  th ;
                     