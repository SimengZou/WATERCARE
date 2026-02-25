
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5DEVICES
                   as                       
                    SELECT
                        t.DEV_CODE,
                        t.DEV_DESC,
                        t.DEV_CATFLAG,
                        t.DEV_RTYPE,
                        t.DEV_TYPE,
                        t.DEV_CATEGORY,
                        t.DEV_DRIVER,
                        t.DEV_MODE,
                        t.DEV_SPECIAL,
                        t.DEV_ORIENTATION,
                        t.DEV_DESTINATION,
                        t.DEV_ORG,
                        t.DEV_UPDATECOUNT,
                        t.DEV_UPDATED,
                        t.DEV_NOTUSED,
                        t.DEV_LASTSAVED,
                        t.DEV_LASTLOGINDATE,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5DEVICES as  t
					 union
					        SELECT
                        th.DEV_CODE,
                        th.DEV_DESC,
                        th.DEV_CATFLAG,
                        th.DEV_RTYPE,
                        th.DEV_TYPE,
                        th.DEV_CATEGORY,
                        th.DEV_DRIVER,
                        th.DEV_MODE,
                        th.DEV_SPECIAL,
                        th.DEV_ORIENTATION,
                        th.DEV_DESTINATION,
                        th.DEV_ORG,
                        th.DEV_UPDATECOUNT,
                        th.DEV_UPDATED,
                        th.DEV_NOTUSED,
                        th.DEV_LASTSAVED,
                        th.DEV_LASTLOGINDATE,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5DEVICES as  th ;
                     