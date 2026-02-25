
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5MAPSCONSENT
                   as                       
                    SELECT
                        t.MCT_APPID,
                        t.MCT_USERID,
                        t.MCT_DEVICEID,
                        t.MCT_PRODUCT,
                        t.MCT_LASTUPDATED,
                        t.MCT_UPDATECOUNT,
                        t.MCT_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5MAPSCONSENT as  t
					 union
					        SELECT
                        th.MCT_APPID,
                        th.MCT_USERID,
                        th.MCT_DEVICEID,
                        th.MCT_PRODUCT,
                        th.MCT_LASTUPDATED,
                        th.MCT_UPDATECOUNT,
                        th.MCT_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5MAPSCONSENT as  th ;
                     