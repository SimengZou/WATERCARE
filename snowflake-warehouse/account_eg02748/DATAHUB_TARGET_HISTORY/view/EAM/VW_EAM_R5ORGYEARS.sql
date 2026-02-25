
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5ORGYEARS
                   as                       
                    SELECT
                        t.OYE_PK,
                        t.OYE_ORG,
                        t.OYE_START,
                        t.OYE_END,
                        t.OYE_UPDATECOUNT,
                        t.OYE_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5ORGYEARS as  t
					 union
					        SELECT
                        th.OYE_PK,
                        th.OYE_ORG,
                        th.OYE_START,
                        th.OYE_END,
                        th.OYE_UPDATECOUNT,
                        th.OYE_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5ORGYEARS as  th ;
                     