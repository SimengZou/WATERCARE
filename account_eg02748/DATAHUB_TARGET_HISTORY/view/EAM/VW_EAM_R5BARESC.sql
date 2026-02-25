
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5BARESC
                   as                       
                    SELECT
                        t.BCE_TEXT1,
                        t.BCE_TEXT2,
                        t.BCE_LINE,
                        t.BCE_COLUMN,
                        t.BCE_ESCAPE,
                        t.BCE_UPDATECOUNT,
                        t.BCE_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5BARESC as  t
					 union
					        SELECT
                        th.BCE_TEXT1,
                        th.BCE_TEXT2,
                        th.BCE_LINE,
                        th.BCE_COLUMN,
                        th.BCE_ESCAPE,
                        th.BCE_UPDATECOUNT,
                        th.BCE_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5BARESC as  th ;
                     