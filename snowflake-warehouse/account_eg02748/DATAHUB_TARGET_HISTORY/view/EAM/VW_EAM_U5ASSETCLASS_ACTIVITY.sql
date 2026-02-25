
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_U5ASSETCLASS_ACTIVITY
                   as                       
                    SELECT
                        t.ACAA_ASSETCLASS,
                        t.ACAA_TASKTYPE,
                        t.ACAA_NOTUSED,
                        t.ACAA_ASSETCLASS_DESC,
                        t.ACAA_TASKTYPE_DESC,
                        t.CREATEDBY,
                        t.CREATED,
                        t.UPDATEDBY,
                        t.UPDATED,
                        t.LASTSAVED,
                        t.UPDATECOUNT,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_U5ASSETCLASS_ACTIVITY as  t
					 union
					        SELECT
                        th.ACAA_ASSETCLASS,
                        th.ACAA_TASKTYPE,
                        th.ACAA_NOTUSED,
                        th.ACAA_ASSETCLASS_DESC,
                        th.ACAA_TASKTYPE_DESC,
                        th.CREATEDBY,
                        th.CREATED,
                        th.UPDATEDBY,
                        th.UPDATED,
                        th.LASTSAVED,
                        th.UPDATECOUNT,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_U5ASSETCLASS_ACTIVITY as  th ;
                     