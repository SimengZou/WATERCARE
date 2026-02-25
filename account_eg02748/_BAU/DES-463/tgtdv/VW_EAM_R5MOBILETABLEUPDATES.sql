
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5MOBILETABLEUPDATES
                   as                       
                    SELECT
                        t.MTU_TABLENAME,
                        t.MTU_UPDATED,
                        t.MTU_RENTITY,
                        t.MTU_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5MOBILETABLEUPDATES as  t
					 union
					        SELECT
                        th.MTU_TABLENAME,
                        th.MTU_UPDATED,
                        th.MTU_RENTITY,
                        th.MTU_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5MOBILETABLEUPDATES as  th ;
                     