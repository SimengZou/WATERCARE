
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5URLPARAMETERS
                   as                       
                    SELECT
                        t.URL_USERFUNCTION,
                        t.URL_TAB,
                        t.URL_PARAMETERNAME,
                        t.URL_ALTERNATEPARAMETERNAME,
                        t.URL_PARAMETERVALUE,
                        t.URL_SYSTEM,
                        t.URL_ACTIVE,
                        t.URL_UPDATECOUNT,
                        t.URL_USEFIELDVALUE,
                        t.URL_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5URLPARAMETERS as  t
					 union
					        SELECT
                        th.URL_USERFUNCTION,
                        th.URL_TAB,
                        th.URL_PARAMETERNAME,
                        th.URL_ALTERNATEPARAMETERNAME,
                        th.URL_PARAMETERVALUE,
                        th.URL_SYSTEM,
                        th.URL_ACTIVE,
                        th.URL_UPDATECOUNT,
                        th.URL_USEFIELDVALUE,
                        th.URL_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5URLPARAMETERS as  th ;
                     