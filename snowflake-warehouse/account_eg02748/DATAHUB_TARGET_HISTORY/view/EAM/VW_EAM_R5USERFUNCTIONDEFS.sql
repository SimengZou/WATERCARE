
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5USERFUNCTIONDEFS
                   as                       
                    SELECT
                        t.UFN_FUNCTIONNAME,
                        t.UFN_DESCRIPTION,
                        t.UFN_UPDATECOUNT,
                        t.UFN_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5USERFUNCTIONDEFS as  t
					 union
					        SELECT
                        th.UFN_FUNCTIONNAME,
                        th.UFN_DESCRIPTION,
                        th.UFN_UPDATECOUNT,
                        th.UFN_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5USERFUNCTIONDEFS as  th ;
                     