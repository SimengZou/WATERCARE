
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5ALERTGRIDDATA
                   as                       
                    SELECT
                        t.AGD_PK,
                        t.AGD_ALERT,
                        t.AGD_GRIDID,
                        t.AGD_DATASPYID,
                        t.AGD_DATE,
                        t.AGD_RECIPIENT,
                        t.AGD_DESCRIPTION,
                        t.AGD_DATA,
                        t.AGD_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5ALERTGRIDDATA as  t
					 union
					        SELECT
                        th.AGD_PK,
                        th.AGD_ALERT,
                        th.AGD_GRIDID,
                        th.AGD_DATASPYID,
                        th.AGD_DATE,
                        th.AGD_RECIPIENT,
                        th.AGD_DESCRIPTION,
                        th.AGD_DATA,
                        th.AGD_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5ALERTGRIDDATA as  th ;
                     