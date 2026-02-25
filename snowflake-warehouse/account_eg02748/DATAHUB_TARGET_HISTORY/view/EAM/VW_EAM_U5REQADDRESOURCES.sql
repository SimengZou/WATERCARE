
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_U5REQADDRESOURCES
                   as                       
                    SELECT
                        t.RAR_TRANSID,
                        t.RAR_EVENT,
                        t.RAR_TYPE,
                        t.RAR_NOTES,
                        t.RAR_HAZARDS,
                        t.RAR_RESULTWONUM,
                        t.RAR_RAISEDBY,
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
          
                     FROM DATAHUB_TARGET.EAM_U5REQADDRESOURCES as  t
					 union
					        SELECT
                        th.RAR_TRANSID,
                        th.RAR_EVENT,
                        th.RAR_TYPE,
                        th.RAR_NOTES,
                        th.RAR_HAZARDS,
                        th.RAR_RESULTWONUM,
                        th.RAR_RAISEDBY,
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
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_U5REQADDRESOURCES as  th ;
                     