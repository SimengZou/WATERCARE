
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_U5JOBPRRESPTIME
                   as                       
                    SELECT
                        t.JBP_WOPRIORITY,
                        t.JBP_WOPRIORITY_DESC,
                        t.JBP_WOTYPE,
                        t.JBP_CONTRACTOR,
                        t.JBP_WORESPONSETIME,
                        t.JBP_WOCOMPLETIONTIME,
                        t.JBP_WORESOLUTIONTIME,
                        t.JBP_BUSINESSDAYS,
                        t.JBP_ORGANIZATION,
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
          
                     FROM DATAHUB_TARGET.EAM_U5JOBPRRESPTIME as  t
					 union
					        SELECT
                        th.JBP_WOPRIORITY,
                        th.JBP_WOPRIORITY_DESC,
                        th.JBP_WOTYPE,
                        th.JBP_CONTRACTOR,
                        th.JBP_WORESPONSETIME,
                        th.JBP_WOCOMPLETIONTIME,
                        th.JBP_WORESOLUTIONTIME,
                        th.JBP_BUSINESSDAYS,
                        th.JBP_ORGANIZATION,
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
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_U5JOBPRRESPTIME as  th ;
                     