
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5SHFDAYS
                   as                       
                    SELECT
                        t.SHD_SHIFT,
                        t.SHD_DAY,
                        t.SHD_TIME,
                        t.SHD_HOURS,
                        t.SHD_UPDATECOUNT,
                        t.SHD_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5SHFDAYS as  t
					 union
					        SELECT
                        th.SHD_SHIFT,
                        th.SHD_DAY,
                        th.SHD_TIME,
                        th.SHD_HOURS,
                        th.SHD_UPDATECOUNT,
                        th.SHD_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5SHFDAYS as  th ;
                     