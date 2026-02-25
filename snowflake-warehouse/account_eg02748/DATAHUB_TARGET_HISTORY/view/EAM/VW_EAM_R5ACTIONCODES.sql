
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5ACTIONCODES
                   as                       
                    SELECT
                        t.ACC_CODE,
                        t.ACC_DESC,
                        t.ACC_CLASS,
                        t.ACC_GEN,
                        t.ACC_CLASS_ORG,
                        t.ACC_UPDATECOUNT,
                        t.ACC_CREATED,
                        t.ACC_UPDATED,
                        t.ACC_PARTFAILURE,
                        t.ACC_NOTUSED,
                        t.ACC_ENABLEWORKORDERS,
                        t.ACC_GROUP,
                        t.ACC_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5ACTIONCODES as  t
					 union
					        SELECT
                        th.ACC_CODE,
                        th.ACC_DESC,
                        th.ACC_CLASS,
                        th.ACC_GEN,
                        th.ACC_CLASS_ORG,
                        th.ACC_UPDATECOUNT,
                        th.ACC_CREATED,
                        th.ACC_UPDATED,
                        th.ACC_PARTFAILURE,
                        th.ACC_NOTUSED,
                        th.ACC_ENABLEWORKORDERS,
                        th.ACC_GROUP,
                        th.ACC_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5ACTIONCODES as  th ;
                     