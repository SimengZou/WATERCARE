
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5PRINTERTYPES
                   as                       
                    SELECT
                        t.PNT_CODE,
                        t.PNT_DESC,
                        t.PNT_UPDATECOUNT,
                        t.PNT_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5PRINTERTYPES as  t
					 union
					        SELECT
                        th.PNT_CODE,
                        th.PNT_DESC,
                        th.PNT_UPDATECOUNT,
                        th.PNT_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5PRINTERTYPES as  th ;
                     