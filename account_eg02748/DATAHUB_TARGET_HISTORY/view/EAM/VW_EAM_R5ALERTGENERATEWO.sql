
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5ALERTGENERATEWO
                   as                       
                    SELECT
                        t.AGW_ALERT,
                        t.AGW_TYPE,
                        t.AGW_GENERATETHROUGHFIELDID,
                        t.AGW_UPDATECOUNT,
                        t.AGW_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5ALERTGENERATEWO as  t
					 union
					        SELECT
                        th.AGW_ALERT,
                        th.AGW_TYPE,
                        th.AGW_GENERATETHROUGHFIELDID,
                        th.AGW_UPDATECOUNT,
                        th.AGW_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5ALERTGENERATEWO as  th ;
                     