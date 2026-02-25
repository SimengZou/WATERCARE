
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5ADDPROPERTIES
                   as                       
                    SELECT
                        t.APR_PROPERTY,
                        t.APR_RENTITY,
                        t.APR_CLASS,
                        t.APR_LINE,
                        t.APR_UOM,
                        t.APR_LIST,
                        t.APR_LISTVALID,
                        t.APR_REQUIRED,
                        t.APR_NONUPDCAT,
                        t.APR_CLASS_ORG,
                        t.APR_WODISP,
                        t.APR_UPDATECOUNT,
                        t.APR_CREATED,
                        t.APR_UPDATED,
                        t.APR_GROUPLABEL,
                        t.APR_LASTSAVED,
                        t.APR_STATUS,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5ADDPROPERTIES as  t
					 union
					        SELECT
                        th.APR_PROPERTY,
                        th.APR_RENTITY,
                        th.APR_CLASS,
                        th.APR_LINE,
                        th.APR_UOM,
                        th.APR_LIST,
                        th.APR_LISTVALID,
                        th.APR_REQUIRED,
                        th.APR_NONUPDCAT,
                        th.APR_CLASS_ORG,
                        th.APR_WODISP,
                        th.APR_UPDATECOUNT,
                        th.APR_CREATED,
                        th.APR_UPDATED,
                        th.APR_GROUPLABEL,
                        th.APR_LASTSAVED,
                        th.APR_STATUS,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5ADDPROPERTIES as  th ;
                     