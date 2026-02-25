
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5CONTRACTS
                   as                       
                    SELECT
                        t.CON_CODE,
                        t.CON_DESC,
                        t.CON_CLASS,
                        t.CON_SUPPLIER,
                        t.CON_LANG,
                        t.CON_CURR,
                        t.CON_COPY,
                        t.CON_STORE,
                        t.CON_STATUS,
                        t.CON_RSTATUS,
                        t.CON_OWN,
                        t.CON_REF,
                        t.CON_CREATE,
                        t.CON_START,
                        t.CON_END,
                        t.CON_RENEW,
                        t.CON_PRINTED,
                        t.CON_OWNCONTACT,
                        t.CON_CONTACT,
                        t.CON_ORG,
                        t.CON_CLASS_ORG,
                        t.CON_SUPPLIER_ORG,
                        t.CON_UPDATECOUNT,
                        t.CON_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5CONTRACTS as  t
					 union
					        SELECT
                        th.CON_CODE,
                        th.CON_DESC,
                        th.CON_CLASS,
                        th.CON_SUPPLIER,
                        th.CON_LANG,
                        th.CON_CURR,
                        th.CON_COPY,
                        th.CON_STORE,
                        th.CON_STATUS,
                        th.CON_RSTATUS,
                        th.CON_OWN,
                        th.CON_REF,
                        th.CON_CREATE,
                        th.CON_START,
                        th.CON_END,
                        th.CON_RENEW,
                        th.CON_PRINTED,
                        th.CON_OWNCONTACT,
                        th.CON_CONTACT,
                        th.CON_ORG,
                        th.CON_CLASS_ORG,
                        th.CON_SUPPLIER_ORG,
                        th.CON_UPDATECOUNT,
                        th.CON_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5CONTRACTS as  th ;
                     