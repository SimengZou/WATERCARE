
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5WSRSPHIST
                   as                       
                    SELECT
                        t.WSR_MESSAGE,
                        t.WSR_PROCESS,
                        t.WSR_TIME,
                        t.WSR_STATUS,
                        t.WSR_RSTATUS,
                        t.WSR_STATUS_MESSAGE,
                        t.WSR_DATAAREA,
                        t.WSR_ADDRESS,
                        t.WSR_UPDATECOUNT,
                        t.WSR_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5WSRSPHIST as  t
					 union
					        SELECT
                        th.WSR_MESSAGE,
                        th.WSR_PROCESS,
                        th.WSR_TIME,
                        th.WSR_STATUS,
                        th.WSR_RSTATUS,
                        th.WSR_STATUS_MESSAGE,
                        th.WSR_DATAAREA,
                        th.WSR_ADDRESS,
                        th.WSR_UPDATECOUNT,
                        th.WSR_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5WSRSPHIST as  th ;
                     