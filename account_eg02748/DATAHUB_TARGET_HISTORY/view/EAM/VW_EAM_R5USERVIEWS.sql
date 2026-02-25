
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5USERVIEWS
                   as                       
                    SELECT
                        t.UVW_CODE,
                        t.UVW_DESC,
                        t.UVW_NAME,
                        t.UVW_NOTE,
                        t.UVW_STMT,
                        t.UVW_ACTIVE,
                        t.UVW_UPDATECOUNT,
                        t.UVW_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5USERVIEWS as  t
					 union
					        SELECT
                        th.UVW_CODE,
                        th.UVW_DESC,
                        th.UVW_NAME,
                        th.UVW_NOTE,
                        th.UVW_STMT,
                        th.UVW_ACTIVE,
                        th.UVW_UPDATECOUNT,
                        th.UVW_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5USERVIEWS as  th ;
                     