
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_U5CONTRACTORS
                   as                       
                    SELECT
                        t.CON_SERVICEAREA,
                        t.CON_CONTRACTORCODE,
                        t.CON_CONTRACTORDESC,
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
          
                     FROM DATAHUB_TARGET.EAM_U5CONTRACTORS as  t
					 union
					        SELECT
                        th.CON_SERVICEAREA,
                        th.CON_CONTRACTORCODE,
                        th.CON_CONTRACTORDESC,
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
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_U5CONTRACTORS as  th ;
                     