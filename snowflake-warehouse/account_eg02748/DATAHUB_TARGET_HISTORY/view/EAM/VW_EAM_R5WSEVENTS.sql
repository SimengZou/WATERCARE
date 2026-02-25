
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5WSEVENTS
                   as                       
                    SELECT
                        t.WSE_CODE,
                        t.WSE_DESC,
                        t.WSE_BASE_EVENT,
                        t.WSE_FILTER_PROCESSOR,
                        t.WSE_MSG_TEMPLATE,
                        t.WSE_UPDATECOUNT,
                        t.WSE_MEKEY,
                        t.WSE_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5WSEVENTS as  t
					 union
					        SELECT
                        th.WSE_CODE,
                        th.WSE_DESC,
                        th.WSE_BASE_EVENT,
                        th.WSE_FILTER_PROCESSOR,
                        th.WSE_MSG_TEMPLATE,
                        th.WSE_UPDATECOUNT,
                        th.WSE_MEKEY,
                        th.WSE_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5WSEVENTS as  th ;
                     