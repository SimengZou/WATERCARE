
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5DWFACTFIELDS
                   as                       
                    SELECT
                        t.FFL_FIELD,
                        t.FFL_DMTTABLE,
                        t.FFL_DESC,
                        t.FFL_ADDITIVITY,
                        t.FFL_BOTNUMBER,
                        t.FFL_FIELDID,
                        t.FFL_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5DWFACTFIELDS as  t
					 union
					        SELECT
                        th.FFL_FIELD,
                        th.FFL_DMTTABLE,
                        th.FFL_DESC,
                        th.FFL_ADDITIVITY,
                        th.FFL_BOTNUMBER,
                        th.FFL_FIELDID,
                        th.FFL_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5DWFACTFIELDS as  th ;
                     