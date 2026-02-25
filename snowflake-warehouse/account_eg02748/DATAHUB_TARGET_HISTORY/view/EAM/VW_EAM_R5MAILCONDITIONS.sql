
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5MAILCONDITIONS
                   as                       
                    SELECT
                        t.MAC_TABLE,
                        t.MAC_TEMPLATE,
                        t.MAC_COLUMN,
                        t.MAC_CRITERIA,
                        t.MAC_VALUE1,
                        t.MAC_VALUE2,
                        t.MAC_UPDATECOUNT,
                        t.MAC_COLUMNGRO,
                        t.MAC_ANDOR,
                        t.MAC_ATTRIBPK,
                        t.MAC_PK,
                        t.MAC_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5MAILCONDITIONS as  t
					 union
					        SELECT
                        th.MAC_TABLE,
                        th.MAC_TEMPLATE,
                        th.MAC_COLUMN,
                        th.MAC_CRITERIA,
                        th.MAC_VALUE1,
                        th.MAC_VALUE2,
                        th.MAC_UPDATECOUNT,
                        th.MAC_COLUMNGRO,
                        th.MAC_ANDOR,
                        th.MAC_ATTRIBPK,
                        th.MAC_PK,
                        th.MAC_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5MAILCONDITIONS as  th ;
                     