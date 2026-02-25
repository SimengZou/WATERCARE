
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5ADDRESS
                   as                       
                    SELECT
                        t.ADR_RENTITY,
                        t.ADR_CODE,
                        t.ADR_RTYPE,
                        t.ADR_TEXT,
                        t.ADR_ADDRESS1,
                        t.ADR_ADDRESS2,
                        t.ADR_ADDRESS3,
                        t.ADR_CITY,
                        t.ADR_STATE,
                        t.ADR_ZIP,
                        t.ADR_COUNTRY,
                        t.ADR_PHONE,
                        t.ADR_PHONEEXTN,
                        t.ADR_FAX,
                        t.ADR_EMAIL,
                        t.ADR_UPDATECOUNT,
                        t.ADR_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5ADDRESS as  t
					 union
					        SELECT
                        th.ADR_RENTITY,
                        th.ADR_CODE,
                        th.ADR_RTYPE,
                        th.ADR_TEXT,
                        th.ADR_ADDRESS1,
                        th.ADR_ADDRESS2,
                        th.ADR_ADDRESS3,
                        th.ADR_CITY,
                        th.ADR_STATE,
                        th.ADR_ZIP,
                        th.ADR_COUNTRY,
                        th.ADR_PHONE,
                        th.ADR_PHONEEXTN,
                        th.ADR_FAX,
                        th.ADR_EMAIL,
                        th.ADR_UPDATECOUNT,
                        th.ADR_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5ADDRESS as  th ;
                     