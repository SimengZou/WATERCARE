
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5JOBTYPEAUTH
                   as                       
                    SELECT
                        t.JTA_GROUP,
                        t.JTA_JOBTYPE,
                        t.JTA_INSERT,
                        t.JTA_UPDATE,
                        t.JTA_DELETE,
                        t.JTA_UPDATECOUNT,
                        t.JTA_UPDATED,
                        t.JTA_STATUS,
                        t.JTA_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5JOBTYPEAUTH as  t
					 union
					        SELECT
                        th.JTA_GROUP,
                        th.JTA_JOBTYPE,
                        th.JTA_INSERT,
                        th.JTA_UPDATE,
                        th.JTA_DELETE,
                        th.JTA_UPDATECOUNT,
                        th.JTA_UPDATED,
                        th.JTA_STATUS,
                        th.JTA_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5JOBTYPEAUTH as  th ;
                     