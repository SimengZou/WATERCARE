
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5PROPERTIES
                   as                       
                    SELECT
                        t.PRO_CODE,
                        t.PRO_TYPE,
                        t.PRO_TEXT,
                        t.PRO_MIN,
                        t.PRO_MAX,
                        t.PRO_RENTITY,
                        t.PRO_UPDATECOUNT,
                        t.PRO_CREATED,
                        t.PRO_UPDATED,
                        t.PRO_INCLUDEINGRIDS,
                        t.PRO_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5PROPERTIES as  t
					 union
					        SELECT
                        th.PRO_CODE,
                        th.PRO_TYPE,
                        th.PRO_TEXT,
                        th.PRO_MIN,
                        th.PRO_MAX,
                        th.PRO_RENTITY,
                        th.PRO_UPDATECOUNT,
                        th.PRO_CREATED,
                        th.PRO_UPDATED,
                        th.PRO_INCLUDEINGRIDS,
                        th.PRO_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5PROPERTIES as  th ;
                     