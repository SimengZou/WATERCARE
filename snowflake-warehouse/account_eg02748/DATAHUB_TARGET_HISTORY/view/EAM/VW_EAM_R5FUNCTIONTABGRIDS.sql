
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5FUNCTIONTABGRIDS
                   as                       
                    SELECT
                        t.FTG_FUNCTION,
                        t.FTG_TAB,
                        t.FTG_GRIDID,
                        t.FTG_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5FUNCTIONTABGRIDS as  t
					 union
					        SELECT
                        th.FTG_FUNCTION,
                        th.FTG_TAB,
                        th.FTG_GRIDID,
                        th.FTG_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5FUNCTIONTABGRIDS as  th ;
                     