
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5PRODUCTVERSIONHISTORY
                   as                       
                    SELECT
                        t.PVH_PRODUCTCODE,
                        t.PVH_CHANGED,
                        t.PVH_DESC,
                        t.PVH_VERSION,
                        t.PVH_BUILD,
                        t.PVH_PATCH,
                        t.PVH_UPDATECOUNT,
                        t.PVH_BUILDDATE,
                        t.PVH_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5PRODUCTVERSIONHISTORY as  t
					 union
					        SELECT
                        th.PVH_PRODUCTCODE,
                        th.PVH_CHANGED,
                        th.PVH_DESC,
                        th.PVH_VERSION,
                        th.PVH_BUILD,
                        th.PVH_PATCH,
                        th.PVH_UPDATECOUNT,
                        th.PVH_BUILDDATE,
                        th.PVH_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5PRODUCTVERSIONHISTORY as  th ;
                     