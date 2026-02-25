
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5ELECSIGN
                   as                       
                    SELECT
                        t.ELS_CODE,
                        t.ELS_ENTCODE,
                        t.ELS_ORG,
                        t.ELS_USER,
                        t.ELS_DATE,
                        t.ELS_ENTITY,
                        t.ELS_SIGNTYPE,
                        t.ELS_STATUS,
                        t.ELS_PARENT,
                        t.ELS_CERTIFYNUM,
                        t.ELS_CERTIFYTYPE,
                        t.ELS_EXTERNALDATE,
                        t.ELS_USERDESC,
                        t.ELS_SIGNTYPEDESC,
                        t.ELS_LASTCHANGED,
                        t.ELS_ENTCODE2,
                        t.ELS_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5ELECSIGN as  t
					 union
					        SELECT
                        th.ELS_CODE,
                        th.ELS_ENTCODE,
                        th.ELS_ORG,
                        th.ELS_USER,
                        th.ELS_DATE,
                        th.ELS_ENTITY,
                        th.ELS_SIGNTYPE,
                        th.ELS_STATUS,
                        th.ELS_PARENT,
                        th.ELS_CERTIFYNUM,
                        th.ELS_CERTIFYTYPE,
                        th.ELS_EXTERNALDATE,
                        th.ELS_USERDESC,
                        th.ELS_SIGNTYPEDESC,
                        th.ELS_LASTCHANGED,
                        th.ELS_ENTCODE2,
                        th.ELS_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5ELECSIGN as  th ;
                     