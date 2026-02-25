
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5ALERTSQL
                   as                       
                    SELECT
                        t.ALS_ALERT,
                        t.ALS_RTYPE,
                        t.ALS_STMT,
                        t.ALS_ABORTONFAILURE,
                        t.ALS_INCLUDEPREVIEW,
                        t.ALS_ACTIVE,
                        t.ALS_COMMENT,
                        t.ALS_UPDATECOUNT,
                        t.ALS_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5ALERTSQL as  t
					 union
					        SELECT
                        th.ALS_ALERT,
                        th.ALS_RTYPE,
                        th.ALS_STMT,
                        th.ALS_ABORTONFAILURE,
                        th.ALS_INCLUDEPREVIEW,
                        th.ALS_ACTIVE,
                        th.ALS_COMMENT,
                        th.ALS_UPDATECOUNT,
                        th.ALS_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5ALERTSQL as  th ;
                     