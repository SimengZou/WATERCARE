
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5ENTITYTABLES
                   as                       
                    SELECT
                        t.ETT_RENTITY,
                        t.ETT_MOS,
                        t.ETT_UPDATECOUNT,
                        t.ETT_UPDATED,
                        t.ETT_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5ENTITYTABLES as  t
					 union
					        SELECT
                        th.ETT_RENTITY,
                        th.ETT_MOS,
                        th.ETT_UPDATECOUNT,
                        th.ETT_UPDATED,
                        th.ETT_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5ENTITYTABLES as  th ;
                     