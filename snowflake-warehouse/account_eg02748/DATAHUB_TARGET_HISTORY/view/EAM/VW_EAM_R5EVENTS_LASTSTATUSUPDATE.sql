
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5EVENTS_LASTSTATUSUPDATE
                   as                       
                    SELECT
                        t.ELU_CODE,
                        t.ELU_LASTSTATUSUPDATE,
                        t.ELU_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5EVENTS_LASTSTATUSUPDATE as  t
					 union
					        SELECT
                        th.ELU_CODE,
                        th.ELU_LASTSTATUSUPDATE,
                        th.ELU_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5EVENTS_LASTSTATUSUPDATE as  th ;
                     