
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5USEGRIDSYSDEFAULT
                   as                       
                    SELECT
                        t.USD_GRIDID,
                        t.USD_USERID,
                        t.USD_UPDATECOUNT,
                        t.USD_DATASPYID,
                        t.USD_DATASPYFILTER,
                        t.USD_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5USEGRIDSYSDEFAULT as  t
					 union
					        SELECT
                        th.USD_GRIDID,
                        th.USD_USERID,
                        th.USD_UPDATECOUNT,
                        th.USD_DATASPYID,
                        th.USD_DATASPYFILTER,
                        th.USD_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5USEGRIDSYSDEFAULT as  th ;
                     