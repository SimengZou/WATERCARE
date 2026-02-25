
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5PREFILTERLOOKUP
                   as                       
                    SELECT
                        t.PFE_PAGENAME,
                        t.PFE_ELEMENTID,
                        t.PFE_FILTERSTRXML,
                        t.PFE_USERFILTER,
                        t.PFE_UPDATECOUNT,
                        t.PFE_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5PREFILTERLOOKUP as  t
					 union
					        SELECT
                        th.PFE_PAGENAME,
                        th.PFE_ELEMENTID,
                        th.PFE_FILTERSTRXML,
                        th.PFE_USERFILTER,
                        th.PFE_UPDATECOUNT,
                        th.PFE_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5PREFILTERLOOKUP as  th ;
                     