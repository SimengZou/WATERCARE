
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5UCODES
                   as                       
                    SELECT
                        t.UCO_ENTITY,
                        t.UCO_RENTITY,
                        t.UCO_CODE,
                        t.UCO_RCODE,
                        t.UCO_SYSTEM,
                        t.UCO_DESC,
                        t.UCO_UPDATECOUNT,
                        t.UCO_CREATED,
                        t.UCO_UPDATED,
                        t.UCO_TEXTCODE,
                        t.UCO_NOTUSED,
                        t.UCO_ICON,
                        t.UCO_ICONPATH,
                        t.UCO_WEIGHT,
                        t.UCO_COLOR,
                        t.UCO_CATEGORY,
                        t.UCO_LASTSAVED,
                        t.UCO_GISDISPATCHRANKING,
                        t.UCO_CUADJUSTMENT,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5UCODES as  t
					 union
					        SELECT
                        th.UCO_ENTITY,
                        th.UCO_RENTITY,
                        th.UCO_CODE,
                        th.UCO_RCODE,
                        th.UCO_SYSTEM,
                        th.UCO_DESC,
                        th.UCO_UPDATECOUNT,
                        th.UCO_CREATED,
                        th.UCO_UPDATED,
                        th.UCO_TEXTCODE,
                        th.UCO_NOTUSED,
                        th.UCO_ICON,
                        th.UCO_ICONPATH,
                        th.UCO_WEIGHT,
                        th.UCO_COLOR,
                        th.UCO_CATEGORY,
                        th.UCO_LASTSAVED,
                        th.UCO_GISDISPATCHRANKING,
                        th.UCO_CUADJUSTMENT,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5UCODES as  th ;
                     