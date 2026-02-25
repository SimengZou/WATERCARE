
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5AUDITPK
                   as                       
                    SELECT
                        t.APK_TABLE,
                        t.APK_SEQNO,
                        t.APK_COLUMN,
                        t.APK_UPDATECOUNT,
                        t.APK_DATATYPE,
                        t.APK_UPDATED,
                        t.APK_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5AUDITPK as  t
					 union
					        SELECT
                        th.APK_TABLE,
                        th.APK_SEQNO,
                        th.APK_COLUMN,
                        th.APK_UPDATECOUNT,
                        th.APK_DATATYPE,
                        th.APK_UPDATED,
                        th.APK_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5AUDITPK as  th ;
                     