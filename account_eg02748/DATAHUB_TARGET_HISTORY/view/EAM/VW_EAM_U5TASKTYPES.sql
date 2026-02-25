
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_U5TASKTYPES
                   as                       
                    SELECT
                        t.DESCRIPTION,
                        t.TSK_CODE,
                        t.TSK_AUTO,
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
          
                     FROM DATAHUB_TARGET.EAM_U5TASKTYPES as  t
					 union
					        SELECT
                        th.DESCRIPTION,
                        th.TSK_CODE,
                        th.TSK_AUTO,
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
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_U5TASKTYPES as  th ;
                     