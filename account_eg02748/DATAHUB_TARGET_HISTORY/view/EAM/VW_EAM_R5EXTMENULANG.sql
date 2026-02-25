
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5EXTMENULANG
                   as                       
                    SELECT
                        t.EML_EXTMENU,
                        t.EML_LANG,
                        t.EML_TEXT,
                        t.EML_UPDATECOUNT,
                        t.EML_TRANS,
                        t.EML_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5EXTMENULANG as  t
					 union
					        SELECT
                        th.EML_EXTMENU,
                        th.EML_LANG,
                        th.EML_TEXT,
                        th.EML_UPDATECOUNT,
                        th.EML_TRANS,
                        th.EML_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5EXTMENULANG as  th ;
                     