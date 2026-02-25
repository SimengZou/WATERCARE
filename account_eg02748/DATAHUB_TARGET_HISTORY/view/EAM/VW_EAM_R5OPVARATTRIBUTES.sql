
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5OPVARATTRIBUTES
                   as                       
                    SELECT
                        t.OAT_ID,
                        t.OAT_DESC,
                        t.OAT_TYPE,
                        t.OAT_LABEL,
                        t.OAT_AUDITUSER,
                        t.OAT_AUDITTIMESTAMP,
                        t.OAT_UPDATECOUNT,
                        t.OAT_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5OPVARATTRIBUTES as  t
					 union
					        SELECT
                        th.OAT_ID,
                        th.OAT_DESC,
                        th.OAT_TYPE,
                        th.OAT_LABEL,
                        th.OAT_AUDITUSER,
                        th.OAT_AUDITTIMESTAMP,
                        th.OAT_UPDATECOUNT,
                        th.OAT_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5OPVARATTRIBUTES as  th ;
                     