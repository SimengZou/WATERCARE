
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5PASSWORDS
                   as                       
                    SELECT
                        t.PWD_USER,
                        t.PWD_PASSWORD,
                        t.PWD_LASTUSED,
                        t.PWD_UPDATECOUNT,
                        t.PWD_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5PASSWORDS as  t
					 union
					        SELECT
                        th.PWD_USER,
                        th.PWD_PASSWORD,
                        th.PWD_LASTUSED,
                        th.PWD_UPDATECOUNT,
                        th.PWD_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5PASSWORDS as  th ;
                     