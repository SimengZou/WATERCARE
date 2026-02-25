
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5EDITIONUCODES
                   as                       
                    SELECT
                        t.EDU_RCODE,
                        t.EDU_CODE,
                        t.EDU_LANG,
                        t.EDU_DESC,
                        t.EDU_RENTITY,
                        t.EDU_MARKET,
                        t.EDU_TEXTCODE,
                        t.EDU_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5EDITIONUCODES as  t
					 union
					        SELECT
                        th.EDU_RCODE,
                        th.EDU_CODE,
                        th.EDU_LANG,
                        th.EDU_DESC,
                        th.EDU_RENTITY,
                        th.EDU_MARKET,
                        th.EDU_TEXTCODE,
                        th.EDU_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5EDITIONUCODES as  th ;
                     