
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5USERDEFINEDFIELDSETUP
                   as                       
                    SELECT
                        t.UDF_RENTITY,
                        t.UDF_FIELD,
                        t.UDF_MIN,
                        t.UDF_MAX,
                        t.UDF_PRINT,
                        t.UDF_UOM,
                        t.UDF_LOOKUPTYPE,
                        t.UDF_LOOKUPVALIDATE,
                        t.UDF_LOOKUPRENTITY,
                        t.UDF_DATETYPE,
                        t.UDF_NUMBERTYPE,
                        t.UDF_CURR,
                        t.UDF_ENABLEFORADDONS,
                        t.UDF_UPDATED,
                        t.UDF_UPDATECOUNT,
                        t.UDF_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5USERDEFINEDFIELDSETUP as  t
					 union
					        SELECT
                        th.UDF_RENTITY,
                        th.UDF_FIELD,
                        th.UDF_MIN,
                        th.UDF_MAX,
                        th.UDF_PRINT,
                        th.UDF_UOM,
                        th.UDF_LOOKUPTYPE,
                        th.UDF_LOOKUPVALIDATE,
                        th.UDF_LOOKUPRENTITY,
                        th.UDF_DATETYPE,
                        th.UDF_NUMBERTYPE,
                        th.UDF_CURR,
                        th.UDF_ENABLEFORADDONS,
                        th.UDF_UPDATED,
                        th.UDF_UPDATECOUNT,
                        th.UDF_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5USERDEFINEDFIELDSETUP as  th ;
                     