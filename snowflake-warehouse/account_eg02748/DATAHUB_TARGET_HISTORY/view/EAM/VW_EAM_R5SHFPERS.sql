
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5SHFPERS
                   as                       
                    SELECT
                        t.SHP_SHIFT,
                        t.SHP_PERSON,
                        t.SHP_START,
                        t.SHP_END,
                        t.SHP_UPDATECOUNT,
                        t.SHP_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5SHFPERS as  t
					 union
					        SELECT
                        th.SHP_SHIFT,
                        th.SHP_PERSON,
                        th.SHP_START,
                        th.SHP_END,
                        th.SHP_UPDATECOUNT,
                        th.SHP_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5SHFPERS as  th ;
                     