
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5DWDIMENSIONS
                   as                       
                    SELECT
                        t.DIM_TABLE,
                        t.DIM_DESC,
                        t.DIM_TABLETYPE,
                        t.DIM_KEYFIELD,
                        t.DIM_CREATEKEYSEQUENCE,
                        t.DIM_SURROGATEKEYLOOKUPTBL,
                        t.DIM_BOTNUMBER,
                        t.DIM_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5DWDIMENSIONS as  t
					 union
					        SELECT
                        th.DIM_TABLE,
                        th.DIM_DESC,
                        th.DIM_TABLETYPE,
                        th.DIM_KEYFIELD,
                        th.DIM_CREATEKEYSEQUENCE,
                        th.DIM_SURROGATEKEYLOOKUPTBL,
                        th.DIM_BOTNUMBER,
                        th.DIM_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5DWDIMENSIONS as  th ;
                     