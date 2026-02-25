
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5USERLPFTYPES
                   as                       
                    SELECT
                        t.LPT_LINEARPREFERENCE,
                        t.LPT_TYPE,
                        t.LPT_RTYPE,
                        t.LPT_SEQUENCE,
                        t.LPT_UPDATECOUNT,
                        t.LPT_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5USERLPFTYPES as  t
					 union
					        SELECT
                        th.LPT_LINEARPREFERENCE,
                        th.LPT_TYPE,
                        th.LPT_RTYPE,
                        th.LPT_SEQUENCE,
                        th.LPT_UPDATECOUNT,
                        th.LPT_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5USERLPFTYPES as  th ;
                     