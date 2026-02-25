
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5CUSTOMTEXTS
                   as                       
                    SELECT
                        t.CTT_POOL,
                        t.CTT_LANG,
                        t.CTT_LENGTH,
                        t.CTT_TEXT,
                        t.CTT_ORIGTEXT,
                        t.CTT_DATE,
                        t.CTT_UPDATECOUNT,
                        t.CTT_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5CUSTOMTEXTS as  t
					 union
					        SELECT
                        th.CTT_POOL,
                        th.CTT_LANG,
                        th.CTT_LENGTH,
                        th.CTT_TEXT,
                        th.CTT_ORIGTEXT,
                        th.CTT_DATE,
                        th.CTT_UPDATECOUNT,
                        th.CTT_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5CUSTOMTEXTS as  th ;
                     