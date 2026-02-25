
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5AUDATTRIBS
                   as                       
                    SELECT
                        t.AAT_CODE,
                        t.AAT_TABLE,
                        t.AAT_COLUMN,
                        t.AAT_TEXT,
                        t.AAT_ENTEREDBY,
                        t.AAT_COMMENT,
                        t.AAT_INSERT,
                        t.AAT_UPDATE,
                        t.AAT_DELETE,
                        t.AAT_UPDATECOUNT,
                        t.AAT_UPDATED,
                        t.AAT_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5AUDATTRIBS as  t
					 union
					        SELECT
                        th.AAT_CODE,
                        th.AAT_TABLE,
                        th.AAT_COLUMN,
                        th.AAT_TEXT,
                        th.AAT_ENTEREDBY,
                        th.AAT_COMMENT,
                        th.AAT_INSERT,
                        th.AAT_UPDATE,
                        th.AAT_DELETE,
                        th.AAT_UPDATECOUNT,
                        th.AAT_UPDATED,
                        th.AAT_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5AUDATTRIBS as  th ;
                     