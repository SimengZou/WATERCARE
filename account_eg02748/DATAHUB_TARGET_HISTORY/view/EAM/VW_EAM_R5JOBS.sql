
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5JOBS
                   as                       
                    SELECT
                        t.JOB_NAME,
                        t.JOB_DESCRIPTION,
                        t.JOB_CLASS,
                        t.JOB_TYPE,
                        t.JOB_UPDATECOUNT,
                        t.JOB_PARTNERID,
                        t.JOB_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5JOBS as  t
					 union
					        SELECT
                        th.JOB_NAME,
                        th.JOB_DESCRIPTION,
                        th.JOB_CLASS,
                        th.JOB_TYPE,
                        th.JOB_UPDATECOUNT,
                        th.JOB_PARTNERID,
                        th.JOB_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5JOBS as  th ;
                     