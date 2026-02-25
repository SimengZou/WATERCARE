
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5USERDEFINEDFIELDLOVVALS
                   as                       
                    SELECT
                        t.UDL_RENTITY,
                        t.UDL_FIELD,
                        t.UDL_CODE,
                        t.UDL_DESC,
                        t.UDL_UPDATED,
                        t.UDL_UPDATECOUNT,
                        t.UDL_NOTUSED,
                        t.UDL_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5USERDEFINEDFIELDLOVVALS as  t
					 union
					        SELECT
                        th.UDL_RENTITY,
                        th.UDL_FIELD,
                        th.UDL_CODE,
                        th.UDL_DESC,
                        th.UDL_UPDATED,
                        th.UDL_UPDATECOUNT,
                        th.UDL_NOTUSED,
                        th.UDL_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5USERDEFINEDFIELDLOVVALS as  th ;
                     