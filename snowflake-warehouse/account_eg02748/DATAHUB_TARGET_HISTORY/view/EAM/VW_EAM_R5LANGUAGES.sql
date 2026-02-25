
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5LANGUAGES
                   as                       
                    SELECT
                        t.LAN_CODE,
                        t.LAN_DESC,
                        t.LAN_CLASS,
                        t.LAN_TXTTYPE,
                        t.LAN_CLASS_ORG,
                        t.LAN_UPDATECOUNT,
                        t.LAN_RSTATUS,
                        t.LAN_LASTCREATED,
                        t.LAN_PROCESSSTART,
                        t.LAN_PROCESSEND,
                        t.LAN_INSTALLED,
                        t.LAN_NOTUSED,
                        t.LAN_AVAILABLE,
                        t.LAN_ERRORMESSAGE,
                        t.LAN_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5LANGUAGES as  t
					 union
					        SELECT
                        th.LAN_CODE,
                        th.LAN_DESC,
                        th.LAN_CLASS,
                        th.LAN_TXTTYPE,
                        th.LAN_CLASS_ORG,
                        th.LAN_UPDATECOUNT,
                        th.LAN_RSTATUS,
                        th.LAN_LASTCREATED,
                        th.LAN_PROCESSSTART,
                        th.LAN_PROCESSEND,
                        th.LAN_INSTALLED,
                        th.LAN_NOTUSED,
                        th.LAN_AVAILABLE,
                        th.LAN_ERRORMESSAGE,
                        th.LAN_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5LANGUAGES as  th ;
                     