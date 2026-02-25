
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5MAILTEMPLATE
                   as                       
                    SELECT
                        t.MAT_CODE,
                        t.MAT_DESC,
                        t.MAT_SUBJECT,
                        t.MAT_TEXT,
                        t.MAT_MAIL,
                        t.MAT_UPDATECOUNT,
                        t.MAT_REPORT,
                        t.MAT_FROMEMAIL,
                        t.MAT_PUSHNOTIFICATION,
                        t.MAT_EMAIL,
                        t.MAT_NOTEBOOKEMAILS,
                        t.MAT_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5MAILTEMPLATE as  t
					 union
					        SELECT
                        th.MAT_CODE,
                        th.MAT_DESC,
                        th.MAT_SUBJECT,
                        th.MAT_TEXT,
                        th.MAT_MAIL,
                        th.MAT_UPDATECOUNT,
                        th.MAT_REPORT,
                        th.MAT_FROMEMAIL,
                        th.MAT_PUSHNOTIFICATION,
                        th.MAT_EMAIL,
                        th.MAT_NOTEBOOKEMAILS,
                        th.MAT_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5MAILTEMPLATE as  th ;
                     