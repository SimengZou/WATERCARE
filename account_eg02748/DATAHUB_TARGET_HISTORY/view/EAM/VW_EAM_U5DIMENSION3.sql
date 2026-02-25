
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_U5DIMENSION3
                   as                       
                    SELECT
                        t.DIM3_CODE,
                        t.DESCRIPTION,
                        t.DIM3_NOTUSED,
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
          
                     FROM DATAHUB_TARGET.EAM_U5DIMENSION3 as  t
					 union
					        SELECT
                        th.DIM3_CODE,
                        th.DESCRIPTION,
                        th.DIM3_NOTUSED,
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
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_U5DIMENSION3 as  th ;
                     