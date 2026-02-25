
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5TABPERMISSIONS
                   as                       
                    SELECT
                        t.TRP_FUNCTION,
                        t.TRP_TAB,
                        t.TRP_GROUP,
                        t.TRP_VISIBLE,
                        t.TRP_SELECT,
                        t.TRP_UPDATE,
                        t.TRP_INSERT,
                        t.TRP_DELETE,
                        t.TRP_SYSREQUIRED,
                        t.TRP_UPDATECOUNT,
                        t.TRP_SEQUENCE,
                        t.TRP_SECURITYDDSPYID,
                        t.TRP_ALTERSAVE,
                        t.TRP_UPDATED,
                        t.TRP_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5TABPERMISSIONS as  t
					 union
					        SELECT
                        th.TRP_FUNCTION,
                        th.TRP_TAB,
                        th.TRP_GROUP,
                        th.TRP_VISIBLE,
                        th.TRP_SELECT,
                        th.TRP_UPDATE,
                        th.TRP_INSERT,
                        th.TRP_DELETE,
                        th.TRP_SYSREQUIRED,
                        th.TRP_UPDATECOUNT,
                        th.TRP_SEQUENCE,
                        th.TRP_SECURITYDDSPYID,
                        th.TRP_ALTERSAVE,
                        th.TRP_UPDATED,
                        th.TRP_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5TABPERMISSIONS as  th ;
                     