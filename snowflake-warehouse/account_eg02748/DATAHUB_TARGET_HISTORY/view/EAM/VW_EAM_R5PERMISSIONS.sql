
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5PERMISSIONS
                   as                       
                    SELECT
                        t.PRM_FUNCTION,
                        t.PRM_GROUP,
                        t.PRM_SELECT,
                        t.PRM_UPDATE,
                        t.PRM_INSERT,
                        t.PRM_DELETE,
                        t.PRM_PRINTER,
                        t.PRM_SCREEN,
                        t.PRM_PRFILE,
                        t.PRM_LOCAL,
                        t.PRM_DEFQUERY,
                        t.PRM_OVERRULE,
                        t.PRM_INPUT,
                        t.PRM_UPDATECOUNT,
                        t.PRM_CREATED,
                        t.PRM_UPDATED,
                        t.PRM_SECURITYDDSPYID,
                        t.PRM_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5PERMISSIONS as  t
					 union
					        SELECT
                        th.PRM_FUNCTION,
                        th.PRM_GROUP,
                        th.PRM_SELECT,
                        th.PRM_UPDATE,
                        th.PRM_INSERT,
                        th.PRM_DELETE,
                        th.PRM_PRINTER,
                        th.PRM_SCREEN,
                        th.PRM_PRFILE,
                        th.PRM_LOCAL,
                        th.PRM_DEFQUERY,
                        th.PRM_OVERRULE,
                        th.PRM_INPUT,
                        th.PRM_UPDATECOUNT,
                        th.PRM_CREATED,
                        th.PRM_UPDATED,
                        th.PRM_SECURITYDDSPYID,
                        th.PRM_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5PERMISSIONS as  th ;
                     