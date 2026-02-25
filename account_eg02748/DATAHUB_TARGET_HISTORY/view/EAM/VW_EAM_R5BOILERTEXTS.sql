
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5BOILERTEXTS
                   as                       
                    SELECT
                        t.BOT_FUNCTION,
                        t.BOT_NUMBER,
                        t.BOT_LENGTH,
                        t.BOT_XPOS,
                        t.BOT_YPOS,
                        t.BOT_LANG,
                        t.BOT_TEXT,
                        t.BOT_DEST,
                        t.BOT_PAGE,
                        t.BOT_FLD1,
                        t.BOT_FLD2,
                        t.BOT_PRTP,
                        t.BOT_LVCR,
                        t.BOT_ADLEN,
                        t.BOT_POOL,
                        t.BOT_CHANGED,
                        t.BOT_DISPLAY,
                        t.BOT_UPDATECOUNT,
                        t.BOT_CREATED,
                        t.BOT_UPDATED,
                        t.BOT_CLONED,
                        t.BOT_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5BOILERTEXTS as  t
					 union
					        SELECT
                        th.BOT_FUNCTION,
                        th.BOT_NUMBER,
                        th.BOT_LENGTH,
                        th.BOT_XPOS,
                        th.BOT_YPOS,
                        th.BOT_LANG,
                        th.BOT_TEXT,
                        th.BOT_DEST,
                        th.BOT_PAGE,
                        th.BOT_FLD1,
                        th.BOT_FLD2,
                        th.BOT_PRTP,
                        th.BOT_LVCR,
                        th.BOT_ADLEN,
                        th.BOT_POOL,
                        th.BOT_CHANGED,
                        th.BOT_DISPLAY,
                        th.BOT_UPDATECOUNT,
                        th.BOT_CREATED,
                        th.BOT_UPDATED,
                        th.BOT_CLONED,
                        th.BOT_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5BOILERTEXTS as  th ;
                     