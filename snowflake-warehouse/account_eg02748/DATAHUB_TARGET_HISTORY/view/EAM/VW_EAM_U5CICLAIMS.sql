
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_U5CICLAIMS
                   as                       
                    SELECT
                        t.CLI_TRANSID,
                        t.CLI_EVENT,
                        t.CLI_CONTRACTOR,
                        t.CLI_LASTITEM,
                        t.CLI_COST,
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
          
                     FROM DATAHUB_TARGET.EAM_U5CICLAIMS as  t
					 union
					        SELECT
                        th.CLI_TRANSID,
                        th.CLI_EVENT,
                        th.CLI_CONTRACTOR,
                        th.CLI_LASTITEM,
                        th.CLI_COST,
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
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_U5CICLAIMS as  th ;
                     