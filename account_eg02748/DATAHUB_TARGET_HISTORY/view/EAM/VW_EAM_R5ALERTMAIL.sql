
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5ALERTMAIL
                   as                       
                    SELECT
                        t.ALM_ALERT,
                        t.ALM_TEMPLATE,
                        t.ALM_DELAY,
                        t.ALM_DELAYUOM,
                        t.ALM_UPDATECOUNT,
                        t.ALM_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5ALERTMAIL as  t
					 union
					        SELECT
                        th.ALM_ALERT,
                        th.ALM_TEMPLATE,
                        th.ALM_DELAY,
                        th.ALM_DELAYUOM,
                        th.ALM_UPDATECOUNT,
                        th.ALM_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5ALERTMAIL as  th ;
                     