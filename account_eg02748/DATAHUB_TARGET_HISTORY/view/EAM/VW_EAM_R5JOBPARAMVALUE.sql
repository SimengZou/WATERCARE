
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5JOBPARAMVALUE
                   as                       
                    SELECT
                        t.JPV_CODE,
                        t.JPV_LINE,
                        t.JPV_KEY,
                        t.JPV_VALUE,
                        t.JPV_UPDATECOUNT,
                        t.JPV_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5JOBPARAMVALUE as  t
					 union
					        SELECT
                        th.JPV_CODE,
                        th.JPV_LINE,
                        th.JPV_KEY,
                        th.JPV_VALUE,
                        th.JPV_UPDATECOUNT,
                        th.JPV_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5JOBPARAMVALUE as  th ;
                     