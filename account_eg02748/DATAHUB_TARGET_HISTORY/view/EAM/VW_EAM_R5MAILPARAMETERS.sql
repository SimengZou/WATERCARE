
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5MAILPARAMETERS
                   as                       
                    SELECT
                        t.MAP_TABLE,
                        t.MAP_TEMPLATE,
                        t.MAP_COLUMN,
                        t.MAP_PARAMETER,
                        t.MAP_UPDATECOUNT,
                        t.MAP_ATTRIBPK,
                        t.MAP_REPORTPARAMETER,
                        t.MAP_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5MAILPARAMETERS as  t
					 union
					        SELECT
                        th.MAP_TABLE,
                        th.MAP_TEMPLATE,
                        th.MAP_COLUMN,
                        th.MAP_PARAMETER,
                        th.MAP_UPDATECOUNT,
                        th.MAP_ATTRIBPK,
                        th.MAP_REPORTPARAMETER,
                        th.MAP_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5MAILPARAMETERS as  th ;
                     