
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5DWDIMENSIONFIELDS
                   as                       
                    SELECT
                        t.DMF_FIELD,
                        t.DMF_DIMTABLE,
                        t.DMF_DESC,
                        t.DMF_BOTNUMBER,
                        t.DMF_FIELDID,
                        t.DMF_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5DWDIMENSIONFIELDS as  t
					 union
					        SELECT
                        th.DMF_FIELD,
                        th.DMF_DIMTABLE,
                        th.DMF_DESC,
                        th.DMF_BOTNUMBER,
                        th.DMF_FIELDID,
                        th.DMF_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5DWDIMENSIONFIELDS as  th ;
                     