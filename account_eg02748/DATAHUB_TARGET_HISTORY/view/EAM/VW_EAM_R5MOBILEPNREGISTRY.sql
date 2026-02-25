
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5MOBILEPNREGISTRY
                   as                       
                    SELECT
                        t.MPN_APPID,
                        t.MPN_DEVICEID,
                        t.MPN_PLATFORM,
                        t.MPN_TOKEN,
                        t.MPN_USER,
                        t.MPN_CREATED,
                        t.MPN_CREATEDBY,
                        t.MPN_UPDATED,
                        t.MPN_UPDATEDBY,
                        t.MPN_UPDATECOUNT,
                        t.MPN_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5MOBILEPNREGISTRY as  t
					 union
					        SELECT
                        th.MPN_APPID,
                        th.MPN_DEVICEID,
                        th.MPN_PLATFORM,
                        th.MPN_TOKEN,
                        th.MPN_USER,
                        th.MPN_CREATED,
                        th.MPN_CREATEDBY,
                        th.MPN_UPDATED,
                        th.MPN_UPDATEDBY,
                        th.MPN_UPDATECOUNT,
                        th.MPN_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5MOBILEPNREGISTRY as  th ;
                     