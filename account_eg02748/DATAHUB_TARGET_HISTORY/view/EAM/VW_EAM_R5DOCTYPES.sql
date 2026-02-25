
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5DOCTYPES
                   as                       
                    SELECT
                        t.DOT_EXT,
                        t.DOT_TYPE,
                        t.DOT_UPDATECOUNT,
                        t.DOT_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5DOCTYPES as  t
					 union
					        SELECT
                        th.DOT_EXT,
                        th.DOT_TYPE,
                        th.DOT_UPDATECOUNT,
                        th.DOT_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5DOCTYPES as  th ;
                     