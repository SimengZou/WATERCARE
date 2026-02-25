
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5TASKPRICES
                   as                       
                    SELECT
                        t.TPR_TASK,
                        t.TPR_REVISION,
                        t.TPR_ORG,
                        t.TPR_PRICE,
                        t.TPR_UPDATECOUNT,
                        t.TPR_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5TASKPRICES as  t
					 union
					        SELECT
                        th.TPR_TASK,
                        th.TPR_REVISION,
                        th.TPR_ORG,
                        th.TPR_PRICE,
                        th.TPR_UPDATECOUNT,
                        th.TPR_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5TASKPRICES as  th ;
                     