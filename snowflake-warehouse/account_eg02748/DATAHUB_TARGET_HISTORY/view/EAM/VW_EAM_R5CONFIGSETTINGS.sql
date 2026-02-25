
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5CONFIGSETTINGS
                   as                       
                    SELECT
                        t.CFS_CODE,
                        t.CFS_GROUP,
                        t.CFS_USER,
                        t.CFS_COMPTYPE,
                        t.CFS_CONFIG,
                        t.CFS_XMLCONFIG,
                        t.CFS_CREATED,
                        t.CFS_UPDATED,
                        t.CFS_UPDATECOUNT,
                        t.CFS_DESK,
                        t.CFS_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5CONFIGSETTINGS as  t
					 union
					        SELECT
                        th.CFS_CODE,
                        th.CFS_GROUP,
                        th.CFS_USER,
                        th.CFS_COMPTYPE,
                        th.CFS_CONFIG,
                        th.CFS_XMLCONFIG,
                        th.CFS_CREATED,
                        th.CFS_UPDATED,
                        th.CFS_UPDATECOUNT,
                        th.CFS_DESK,
                        th.CFS_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5CONFIGSETTINGS as  th ;
                     