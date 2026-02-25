
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5ALERTHISTORY
                   as                       
                    SELECT
                        t.CSA_ORG,
                        t.CSA_SCHEDULEITEM,
                        t.CSA_PROJECT,
                        t.CSA_ACTIVITY,
                        t.CSA_CONTRACTCODE,
                        t.CSA_SUPPLIER,
                        t.CSA_CONTRACT,
                        t.CSA_CONTRACTOR,
                        t.CREATEDBY,
                        t.UPDATEDBY,
                        t.UPDATED,
                        t.CREATED,
                        t.UPDATECOUNT,
						t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5ALERTHISTORY as  t
					 union
					        SELECT
						th.CSA_ORG,
                        th.CSA_SCHEDULEITEM,
                        th.CSA_PROJECT,
                        th.CSA_ACTIVITY,
                        th.CSA_CONTRACTCODE,
                        th.CSA_SUPPLIER,
                        th.CSA_CONTRACT,
                        th.CSA_CONTRACTOR,
                        th.CREATEDBY,
                        th.UPDATEDBY,
                        th.UPDATED,
                        th.CREATED,
                        th.UPDATECOUNT,
						th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5ALERTHISTORY as  th ;
                     