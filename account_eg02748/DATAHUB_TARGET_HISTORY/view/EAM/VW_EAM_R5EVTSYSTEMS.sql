
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5EVTSYSTEMS
                   as                       
                    SELECT
                        t.ESY_EVENT,
                        t.ESY_SYSTEM,
                        t.ESY_SYSTEM_ORG,
                        t.ESY_UPDATECOUNT,
                        t.ESY_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5EVTSYSTEMS as  t
					 union
					        SELECT
                        th.ESY_EVENT,
                        th.ESY_SYSTEM,
                        th.ESY_SYSTEM_ORG,
                        th.ESY_UPDATECOUNT,
                        th.ESY_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5EVTSYSTEMS as  th ;
                     