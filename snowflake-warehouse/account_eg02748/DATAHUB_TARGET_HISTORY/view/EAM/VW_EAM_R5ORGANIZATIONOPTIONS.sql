
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5ORGANIZATIONOPTIONS
                   as                       
                    SELECT
                        t.OPA_CODE,
                        t.OPA_ORG,
                        t.OPA_SYSTEM,
                        t.OPA_DESC,
                        t.OPA_COMMENT,
                        t.OPA_FIXED,
                        t.OPA_MODULE,
                        t.OPA_TYPE,
                        t.OPA_VALIDVALUES,
                        t.OPA_UPDATECOUNT,
                        t.OPA_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5ORGANIZATIONOPTIONS as  t
					 union
					        SELECT
                        th.OPA_CODE,
                        th.OPA_ORG,
                        th.OPA_SYSTEM,
                        th.OPA_DESC,
                        th.OPA_COMMENT,
                        th.OPA_FIXED,
                        th.OPA_MODULE,
                        th.OPA_TYPE,
                        th.OPA_VALIDVALUES,
                        th.OPA_UPDATECOUNT,
                        th.OPA_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5ORGANIZATIONOPTIONS as  th ;
                     