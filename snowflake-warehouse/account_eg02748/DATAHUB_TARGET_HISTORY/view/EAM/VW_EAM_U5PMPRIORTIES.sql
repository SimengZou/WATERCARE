
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_U5PMPRIORTIES
                   as                       
                    SELECT
                        t.PMP_CYCLE_LENGTH,
                        t.PMP_CYCLE_UOM,
                        t.PMP_CYCLE_TEXT,
                        t.PMP_RELEASE_DAYS,
                        t.PMP_PRIORITY,
                        t.PMP_COMPLETE_DAYS,
                        t.CREATEDBY,
                        t.CREATED,
                        t.UPDATEDBY,
                        t.UPDATED,
                        t.UPDATECOUNT,
                        t.LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_U5PMPRIORTIES as  t
					 union
					        SELECT
                        th.PMP_CYCLE_LENGTH,
                        th.PMP_CYCLE_UOM,
                        th.PMP_CYCLE_TEXT,
                        th.PMP_RELEASE_DAYS,
                        th.PMP_PRIORITY,
                        th.PMP_COMPLETE_DAYS,
                        th.CREATEDBY,
                        th.CREATED,
                        th.UPDATEDBY,
                        th.UPDATED,
                        th.UPDATECOUNT,
                        th.LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_U5PMPRIORTIES as  th ;
                     