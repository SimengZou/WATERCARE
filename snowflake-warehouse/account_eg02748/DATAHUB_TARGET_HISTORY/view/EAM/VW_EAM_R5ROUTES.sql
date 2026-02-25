
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5ROUTES
                   as                       
                    SELECT
                        t.ROU_CODE,
                        t.ROU_DESC,
                        t.ROU_CATEGORY,
                        t.ROU_TEMPLATE,
                        t.ROU_CLASS,
                        t.ROU_REVISION,
                        t.ROU_REVRSTATUS,
                        t.ROU_REVSTATUS,
                        t.ROU_ORG,
                        t.ROU_CLASS_ORG,
                        t.ROU_UPDATECOUNT,
                        t.ROU_LINKGISWO,
                        t.ROU_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5ROUTES as  t
					 union
					        SELECT
                        th.ROU_CODE,
                        th.ROU_DESC,
                        th.ROU_CATEGORY,
                        th.ROU_TEMPLATE,
                        th.ROU_CLASS,
                        th.ROU_REVISION,
                        th.ROU_REVRSTATUS,
                        th.ROU_REVSTATUS,
                        th.ROU_ORG,
                        th.ROU_CLASS_ORG,
                        th.ROU_UPDATECOUNT,
                        th.ROU_LINKGISWO,
                        th.ROU_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5ROUTES as  th ;
                     