
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5MAILATTRIBS
                   as                       
                    SELECT
                        t.MAA_TABLE,
                        t.MAA_TEMPLATE,
                        t.MAA_ENTEREDBY,
                        t.MAA_COMMENT,
                        t.MAA_INSERT,
                        t.MAA_UPDATE,
                        t.MAA_DELETE,
                        t.MAA_OLDSTATUS,
                        t.MAA_NEWSTATUS,
                        t.MAA_WORKFLOW,
                        t.MAA_UPDATECOUNT,
                        t.MAA_PK,
                        t.MAA_ACTIVE,
                        t.MAA_INCLUDEURL,
                        t.MAA_INCLUDEATTACHMENT,
                        t.MAA_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5MAILATTRIBS as  t
					 union
					        SELECT
                        th.MAA_TABLE,
                        th.MAA_TEMPLATE,
                        th.MAA_ENTEREDBY,
                        th.MAA_COMMENT,
                        th.MAA_INSERT,
                        th.MAA_UPDATE,
                        th.MAA_DELETE,
                        th.MAA_OLDSTATUS,
                        th.MAA_NEWSTATUS,
                        th.MAA_WORKFLOW,
                        th.MAA_UPDATECOUNT,
                        th.MAA_PK,
                        th.MAA_ACTIVE,
                        th.MAA_INCLUDEURL,
                        th.MAA_INCLUDEATTACHMENT,
                        th.MAA_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5MAILATTRIBS as  th ;
                     