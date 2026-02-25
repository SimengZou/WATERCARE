
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5ALLTEXTS
                   as                       
                    SELECT
                        t.ATX_CODE,
                        t.ATX_TEXTTYPE,
                        t.ATX_LANG,
                        t.ATX_TEXT,
                        t.ATX_LASTMODIFIED,
                        t.ATX_UPDATECOUNT,
                        t.ATX_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5ALLTEXTS as  t
					 union
					        SELECT
                        th.ATX_CODE,
                        th.ATX_TEXTTYPE,
                        th.ATX_LANG,
                        th.ATX_TEXT,
                        th.ATX_LASTMODIFIED,
                        th.ATX_UPDATECOUNT,
                        th.ATX_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5ALLTEXTS as  th ;
                     