
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5MOBILEQUERIES
                   as                       
                    SELECT
                        t.MQR_GRIDNAME,
                        t.MQR_VERSION,
                        t.MQR_TABLENAME,
                        t.MQR_CREATETABLE,
                        t.MQR_SQLTEXT,
                        t.MQR_UPDATED,
                        t.MQR_PREPAREGRID,
                        t.MQR_COLUMNMAP,
                        t.MQR_SINGLETHREADPERCONN,
                        t.MQR_PREPARETABLEUSED,
                        t.MQR_UPDATECOUNT,
                        t.MQR_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5MOBILEQUERIES as  t
					 union
					        SELECT
                        th.MQR_GRIDNAME,
                        th.MQR_VERSION,
                        th.MQR_TABLENAME,
                        th.MQR_CREATETABLE,
                        th.MQR_SQLTEXT,
                        th.MQR_UPDATED,
                        th.MQR_PREPAREGRID,
                        th.MQR_COLUMNMAP,
                        th.MQR_SINGLETHREADPERCONN,
                        th.MQR_PREPARETABLEUSED,
                        th.MQR_UPDATECOUNT,
                        th.MQR_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5MOBILEQUERIES as  th ;
                     