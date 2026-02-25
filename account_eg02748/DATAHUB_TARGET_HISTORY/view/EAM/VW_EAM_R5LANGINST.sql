
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5LANGINST
                   as                       
                    SELECT
                        t.LIN_CODE,
                        t.LIN_TXTTYPE,
                        t.LIN_UPDATECOUNT,
                        t.LIN_LANGFILE,
                        t.LIN_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5LANGINST as  t
					 union
					        SELECT
                        th.LIN_CODE,
                        th.LIN_TXTTYPE,
                        th.LIN_UPDATECOUNT,
                        th.LIN_LANGFILE,
                        th.LIN_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5LANGINST as  th ;
                     