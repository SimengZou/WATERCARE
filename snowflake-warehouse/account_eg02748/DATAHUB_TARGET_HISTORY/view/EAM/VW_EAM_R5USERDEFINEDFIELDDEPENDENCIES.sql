
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5USERDEFINEDFIELDDEPENDENCIES
                   as                       
                    SELECT
                        t.UFD_RENTITY,
                        t.UFD_PAGENAME,
                        t.UFD_GRIDCACHE,
                        t.UFD_LAYOUTCACHE,
                        t.UFD_UPDATECOUNT,
                        t.UFD_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5USERDEFINEDFIELDDEPENDENCIES as  t
					 union
					        SELECT
                        th.UFD_RENTITY,
                        th.UFD_PAGENAME,
                        th.UFD_GRIDCACHE,
                        th.UFD_LAYOUTCACHE,
                        th.UFD_UPDATECOUNT,
                        th.UFD_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5USERDEFINEDFIELDDEPENDENCIES as  th ;
                     