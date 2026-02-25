
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5DRIVERCTRL
                   as                       
                    SELECT
                        t.DRV_CODE,
                        t.DRV_QUEUE,
                        t.DRV_ACTION,
                        t.DRV_LASTRAN,
                        t.DRV_NEXTRUN,
                        t.DRV_FREQUENCY,
                        t.DRV_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5DRIVERCTRL as  t
					 union
					        SELECT
                        th.DRV_CODE,
                        th.DRV_QUEUE,
                        th.DRV_ACTION,
                        th.DRV_LASTRAN,
                        th.DRV_NEXTRUN,
                        th.DRV_FREQUENCY,
                        th.DRV_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5DRIVERCTRL as  th ;
                     