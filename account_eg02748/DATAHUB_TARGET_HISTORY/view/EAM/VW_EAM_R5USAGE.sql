
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5USAGE
                   as                       
                    SELECT
                        t.SUS_ID,
                        t.SUS_ACTION,
                        t.SUS_TYPE,
                        t.SUS_SUBTYPE,
                        t.SUS_TARGET,
                        t.SUS_TARGET_PARENT,
                        t.SUS_TARGET_TAB,
                        t.SUS_USERID,
                        t.SUS_TENANT,
                        t.SUS_SERVERID,
                        t.SUS_SESSIONID,
                        t.SUS_USERAGENT,
                        t.SUS_PROCESSING_TIME,
                        t.SUS_ACTION_DATE,
                        t.SUS_UPDATECOUNT,
                        t.SUS_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5USAGE as  t
					 union
					        SELECT
                        th.SUS_ID,
                        th.SUS_ACTION,
                        th.SUS_TYPE,
                        th.SUS_SUBTYPE,
                        th.SUS_TARGET,
                        th.SUS_TARGET_PARENT,
                        th.SUS_TARGET_TAB,
                        th.SUS_USERID,
                        th.SUS_TENANT,
                        th.SUS_SERVERID,
                        th.SUS_SESSIONID,
                        th.SUS_USERAGENT,
                        th.SUS_PROCESSING_TIME,
                        th.SUS_ACTION_DATE,
                        th.SUS_UPDATECOUNT,
                        th.SUS_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5USAGE as  th ;
                     