
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5SCWORKORDERCOST
                   as                       
                    SELECT
                        t.CWS_ORG,
                        t.CWS_JOBTYPE,
                        t.CWS_DATE,
                        t.CWS_COST,
                        t.CWS_COSTDEFCURR,
                        t.CWS_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5SCWORKORDERCOST as  t
					 union
					        SELECT
                        th.CWS_ORG,
                        th.CWS_JOBTYPE,
                        th.CWS_DATE,
                        th.CWS_COST,
                        th.CWS_COSTDEFCURR,
                        th.CWS_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5SCWORKORDERCOST as  th ;
                     