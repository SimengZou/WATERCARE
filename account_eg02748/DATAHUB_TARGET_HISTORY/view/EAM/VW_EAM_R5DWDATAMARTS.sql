
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5DWDATAMARTS
                   as                       
                    SELECT
                        t.DMT_TABLE,
                        t.DMT_DESC,
                        t.DMT_GRAIN,
                        t.DMT_TABLETYPE,
                        t.DMT_BOTNUMBER,
                        t.DMT_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5DWDATAMARTS as  t
					 union
					        SELECT
                        th.DMT_TABLE,
                        th.DMT_DESC,
                        th.DMT_GRAIN,
                        th.DMT_TABLETYPE,
                        th.DMT_BOTNUMBER,
                        th.DMT_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5DWDATAMARTS as  th ;
                     