
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_U5WAREHOUSES
                   as                       
                    SELECT
                        t.WRH_TRANS,
                        t.WRH_CODE,
                        t.WRH_DESC,
                        t.WRH_ENTERPRISELOCATION,
                        t.WRH_ACCOUNTINGENTITY,
                        t.CREATEDBY,
                        t.CREATED,
                        t.UPDATEDBY,
                        t.UPDATED,
                        t.LASTSAVED,
                        t.UPDATECOUNT,
                        t.WRH_LOCATIONS,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_U5WAREHOUSES as  t
					 union
					        SELECT
                        th.WRH_TRANS,
                        th.WRH_CODE,
                        th.WRH_DESC,
                        th.WRH_ENTERPRISELOCATION,
                        th.WRH_ACCOUNTINGENTITY,
                        th.CREATEDBY,
                        th.CREATED,
                        th.UPDATEDBY,
                        th.UPDATED,
                        th.LASTSAVED,
                        th.UPDATECOUNT,
                        th.WRH_LOCATIONS,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_U5WAREHOUSES as  th ;
                     