
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5DESCRIPTIONS
                   as                       
                    SELECT
                        t.DES_ENTITY,
                        t.DES_RENTITY,
                        t.DES_TYPE,
                        t.DES_RTYPE,
                        t.DES_CODE,
                        t.DES_LANG,
                        t.DES_TEXT,
                        t.DES_TRANS,
                        t.DES_ORG,
                        t.DES_UPDATECOUNT,
                        t.DES_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5DESCRIPTIONS as  t
					 union
					        SELECT
                        th.DES_ENTITY,
                        th.DES_RENTITY,
                        th.DES_TYPE,
                        th.DES_RTYPE,
                        th.DES_CODE,
                        th.DES_LANG,
                        th.DES_TEXT,
                        th.DES_TRANS,
                        th.DES_ORG,
                        th.DES_UPDATECOUNT,
                        th.DES_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5DESCRIPTIONS as  th ;
                     