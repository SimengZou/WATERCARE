
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5PRODUCTVERSION
                   as                       
                    SELECT
                        t.PVS_PRODUCTCODE,
                        t.PVS_PRODUCTDESC,
                        t.PVS_VERSION,
                        t.PVS_BUILD,
                        t.PVS_PATCH,
                        t.PVS_UPDATECOUNT,
                        t.PVS_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5PRODUCTVERSION as  t
					 union
					        SELECT
                        th.PVS_PRODUCTCODE,
                        th.PVS_PRODUCTDESC,
                        th.PVS_VERSION,
                        th.PVS_BUILD,
                        th.PVS_PATCH,
                        th.PVS_UPDATECOUNT,
                        th.PVS_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5PRODUCTVERSION as  th ;
                     