
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5WSREQHIST
                   as                       
                    SELECT
                        t.WSQ_MESSAGE,
                        t.WSQ_PROCESS,
                        t.WSQ_TIME,
                        t.WSQ_STATUS,
                        t.WSQ_RSTATUS,
                        t.WSQ_STATUS_MESSAGE,
                        t.WSQ_DATAAREA,
                        t.WSQ_UPDATECOUNT,
                        t.WSQ_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5WSREQHIST as  t
					 union
					        SELECT
                        th.WSQ_MESSAGE,
                        th.WSQ_PROCESS,
                        th.WSQ_TIME,
                        th.WSQ_STATUS,
                        th.WSQ_RSTATUS,
                        th.WSQ_STATUS_MESSAGE,
                        th.WSQ_DATAAREA,
                        th.WSQ_UPDATECOUNT,
                        th.WSQ_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5WSREQHIST as  th ;
                     