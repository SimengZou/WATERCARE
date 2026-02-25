
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_U5CLAIMSETUP
                   as                       
                    SELECT
                        t.CLS_CODE,
                        t.CLS_COSTITEM,
                        t.CLS_CONTRACT,
                        t.CREATEDBY,
                        t.CREATED,
                        t.UPDATEDBY,
                        t.UPDATED,
                        t.LASTSAVED,
                        t.UPDATECOUNT,
                        t.CLS_STAGINGCOSTCENTER,
                        t.CLS_SUPPLIER,
                        t.CLS_PROJECT,
                        t.CLS_PROJACTIVITY,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_U5CLAIMSETUP as  t
					 union
					        SELECT
                        th.CLS_CODE,
                        th.CLS_COSTITEM,
                        th.CLS_CONTRACT,
                        th.CREATEDBY,
                        th.CREATED,
                        th.UPDATEDBY,
                        th.UPDATED,
                        th.LASTSAVED,
                        th.UPDATECOUNT,
                        th.CLS_STAGINGCOSTCENTER,
                        th.CLS_SUPPLIER,
                        th.CLS_PROJECT,
                        th.CLS_PROJACTIVITY,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_U5CLAIMSETUP as  th ;
                     