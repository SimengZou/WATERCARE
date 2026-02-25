
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5CURRENCIES
                   as                       
                    SELECT
                        t.CUR_CODE,
                        t.CUR_DESC,
                        t.CUR_CLASS,
                        t.CUR_SOURCESYSTEM,
                        t.CUR_SOURCECODE,
                        t.CUR_INTERFACE,
                        t.CUR_NOTUSED,
                        t.CUR_DUAL,
                        t.CUR_CLASS_ORG,
                        t.CUR_UPDATECOUNT,
                        t.CUR_CREATED,
                        t.CUR_UPDATED,
                        t.CUR_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5CURRENCIES as  t
					 union
					        SELECT
                        th.CUR_CODE,
                        th.CUR_DESC,
                        th.CUR_CLASS,
                        th.CUR_SOURCESYSTEM,
                        th.CUR_SOURCECODE,
                        th.CUR_INTERFACE,
                        th.CUR_NOTUSED,
                        th.CUR_DUAL,
                        th.CUR_CLASS_ORG,
                        th.CUR_UPDATECOUNT,
                        th.CUR_CREATED,
                        th.CUR_UPDATED,
                        th.CUR_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5CURRENCIES as  th ;
                     