
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5CLASSES
                   as                       
                    SELECT
                        t.CLS_ENTITY,
                        t.CLS_RENTITY,
                        t.CLS_CODE,
                        t.CLS_DESC,
                        t.CLS_ORG,
                        t.CLS_RENTITYCODE,
                        t.CLS_LEVEL,
                        t.CLS_UPDATECOUNT,
                        t.CLS_CREATED,
                        t.CLS_UPDATED,
                        t.CLS_NOTUSED,
                        t.CLS_ICON,
                        t.CLS_ICONPATH,
                        t.CLS_PROPERTY_DEFINITION,
                        t.CLS_MOBILE_NOTEBOOK_DEFINITION,
                        t.CLS_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5CLASSES as  t
					 union
					        SELECT
                        th.CLS_ENTITY,
                        th.CLS_RENTITY,
                        th.CLS_CODE,
                        th.CLS_DESC,
                        th.CLS_ORG,
                        th.CLS_RENTITYCODE,
                        th.CLS_LEVEL,
                        th.CLS_UPDATECOUNT,
                        th.CLS_CREATED,
                        th.CLS_UPDATED,
                        th.CLS_NOTUSED,
                        th.CLS_ICON,
                        th.CLS_ICONPATH,
                        th.CLS_PROPERTY_DEFINITION,
                        th.CLS_MOBILE_NOTEBOOK_DEFINITION,
                        th.CLS_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5CLASSES as  th ;
                     