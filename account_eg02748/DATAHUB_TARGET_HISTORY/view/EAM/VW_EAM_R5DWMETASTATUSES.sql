
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5DWMETASTATUSES
                   as                       
                    SELECT
                        t.DMS_TABLE,
                        t.DMS_FIELD,
                        t.DMS_STATUSENTITY,
                        t.DMS_ENTITY,
                        t.DMS_STATUSENTITYBOT,
                        t.DMS_ENTITYBOT,
                        t.DMS_UPDATED,
                        t.DMS_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5DWMETASTATUSES as  t
					 union
					        SELECT
                        th.DMS_TABLE,
                        th.DMS_FIELD,
                        th.DMS_STATUSENTITY,
                        th.DMS_ENTITY,
                        th.DMS_STATUSENTITYBOT,
                        th.DMS_ENTITYBOT,
                        th.DMS_UPDATED,
                        th.DMS_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5DWMETASTATUSES as  th ;
                     