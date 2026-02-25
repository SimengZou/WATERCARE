
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5AVAILABILITY
                   as                       
                    SELECT
                        t.AVL_DATE,
                        t.AVL_OWNHOURS,
                        t.AVL_NETHOURS,
                        t.AVL_HIRHOURS,
                        t.AVL_NETHIRED,
                        t.AVL_SPECIAL,
                        t.AVL_MRC,
                        t.AVL_TRADE,
                        t.AVL_UPDATECOUNT,
                        t.AVL_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5AVAILABILITY as  t
					 union
					        SELECT
                        th.AVL_DATE,
                        th.AVL_OWNHOURS,
                        th.AVL_NETHOURS,
                        th.AVL_HIRHOURS,
                        th.AVL_NETHIRED,
                        th.AVL_SPECIAL,
                        th.AVL_MRC,
                        th.AVL_TRADE,
                        th.AVL_UPDATECOUNT,
                        th.AVL_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5AVAILABILITY as  th ;
                     