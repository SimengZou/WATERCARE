
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5VIOLATIONS
                   as                       
                    SELECT
                        t.VIO_USER,
                        t.VIO_PASSWORD,
                        t.VIO_DATE,
                        t.VIO_OSUSER,
                        t.VIO_OSMACHINE,
                        t.VIO_UPDATECOUNT,
                        t.VIO_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5VIOLATIONS as  t
					 union
					        SELECT
                        th.VIO_USER,
                        th.VIO_PASSWORD,
                        th.VIO_DATE,
                        th.VIO_OSUSER,
                        th.VIO_OSMACHINE,
                        th.VIO_UPDATECOUNT,
                        th.VIO_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5VIOLATIONS as  th ;
                     