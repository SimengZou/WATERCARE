
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5UOMS
                   as                       
                    SELECT
                        t.UOM_CODE,
                        t.UOM_DESC,
                        t.UOM_CLASS,
                        t.UOM_NOTUSED,
                        t.UOM_CLASS_ORG,
                        t.UOM_UPDATECOUNT,
                        t.UOM_CREATED,
                        t.UOM_UPDATED,
                        t.UOM_SOAUOM,
                        t.UOM_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5UOMS as  t
					 union
					        SELECT
                        th.UOM_CODE,
                        th.UOM_DESC,
                        th.UOM_CLASS,
                        th.UOM_NOTUSED,
                        th.UOM_CLASS_ORG,
                        th.UOM_UPDATECOUNT,
                        th.UOM_CREATED,
                        th.UOM_UPDATED,
                        th.UOM_SOAUOM,
                        th.UOM_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5UOMS as  th ;
                     