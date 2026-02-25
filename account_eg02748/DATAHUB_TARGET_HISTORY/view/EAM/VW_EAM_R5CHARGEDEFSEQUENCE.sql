
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5CHARGEDEFSEQUENCE
                   as                       
                    SELECT
                        t.CDS_CATEGORY,
                        t.CDS_SUBCATEGORY,
                        t.CDS_LEVEL,
                        t.CDS_ACTUALSUBCAT,
                        t.CDS_SEQUENCE,
                        t.CDS_UPDATECOUNT,
                        t.CDS_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5CHARGEDEFSEQUENCE as  t
					 union
					        SELECT
                        th.CDS_CATEGORY,
                        th.CDS_SUBCATEGORY,
                        th.CDS_LEVEL,
                        th.CDS_ACTUALSUBCAT,
                        th.CDS_SEQUENCE,
                        th.CDS_UPDATECOUNT,
                        th.CDS_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5CHARGEDEFSEQUENCE as  th ;
                     