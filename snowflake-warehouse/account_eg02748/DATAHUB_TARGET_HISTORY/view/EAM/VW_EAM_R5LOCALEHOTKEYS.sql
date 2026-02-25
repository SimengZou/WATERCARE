
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5LOCALEHOTKEYS
                   as                       
                    SELECT
                        t.LHK_LOCALE,
                        t.LHK_CODE,
                        t.LHK_DESC,
                        t.LHK_DEFAULT,
                        t.LHK_KEY,
                        t.LHK_EXTRA,
                        t.LHK_UPDATECOUNT,
                        t.LHK_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5LOCALEHOTKEYS as  t
					 union
					        SELECT
                        th.LHK_LOCALE,
                        th.LHK_CODE,
                        th.LHK_DESC,
                        th.LHK_DEFAULT,
                        th.LHK_KEY,
                        th.LHK_EXTRA,
                        th.LHK_UPDATECOUNT,
                        th.LHK_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5LOCALEHOTKEYS as  th ;
                     