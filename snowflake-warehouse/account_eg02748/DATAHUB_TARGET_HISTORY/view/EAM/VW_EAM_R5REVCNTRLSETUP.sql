
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5REVCNTRLSETUP
                   as                       
                    SELECT
                        t.RCS_PAGENAME,
                        t.RCS_ELEMENTID,
                        t.RCS_XPATH,
                        t.RCS_PROTECTED,
                        t.RCS_UPDATECOUNT,
                        t.RCS_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5REVCNTRLSETUP as  t
					 union
					        SELECT
                        th.RCS_PAGENAME,
                        th.RCS_ELEMENTID,
                        th.RCS_XPATH,
                        th.RCS_PROTECTED,
                        th.RCS_UPDATECOUNT,
                        th.RCS_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5REVCNTRLSETUP as  th ;
                     