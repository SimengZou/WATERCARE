
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5REQUIRCODES
                   as                       
                    SELECT
                        t.RQM_CODE,
                        t.RQM_DESC,
                        t.RQM_CLASS,
                        t.RQM_GEN,
                        t.RQM_CLASS_ORG,
                        t.RQM_UPDATECOUNT,
                        t.RQM_CREATED,
                        t.RQM_UPDATED,
                        t.RQM_PARTFAILURE,
                        t.RQM_NOTUSED,
                        t.RQM_ENABLEWORKORDERS,
                        t.RQM_GROUP,
                        t.RQM_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5REQUIRCODES as  t
					 union
					        SELECT
                        th.RQM_CODE,
                        th.RQM_DESC,
                        th.RQM_CLASS,
                        th.RQM_GEN,
                        th.RQM_CLASS_ORG,
                        th.RQM_UPDATECOUNT,
                        th.RQM_CREATED,
                        th.RQM_UPDATED,
                        th.RQM_PARTFAILURE,
                        th.RQM_NOTUSED,
                        th.RQM_ENABLEWORKORDERS,
                        th.RQM_GROUP,
                        th.RQM_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5REQUIRCODES as  th ;
                     