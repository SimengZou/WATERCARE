
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5ALERTGRIDPARAMS
                   as                       
                    SELECT
                        t.AGP_ALERT,
                        t.AGP_PARAM,
                        t.AGP_VALUE,
                        t.AGP_NVALUE,
                        t.AGP_DVALUE,
                        t.AGP_UPDATECOUNT,
                        t.AGP_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5ALERTGRIDPARAMS as  t
					 union
					        SELECT
                        th.AGP_ALERT,
                        th.AGP_PARAM,
                        th.AGP_VALUE,
                        th.AGP_NVALUE,
                        th.AGP_DVALUE,
                        th.AGP_UPDATECOUNT,
                        th.AGP_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5ALERTGRIDPARAMS as  th ;
                     