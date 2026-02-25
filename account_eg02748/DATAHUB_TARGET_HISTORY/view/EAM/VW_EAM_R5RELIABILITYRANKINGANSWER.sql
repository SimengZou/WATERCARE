
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5RELIABILITYRANKINGANSWER
                   as                       
                    SELECT
                        t.RRW_PK,
                        t.RRW_LEVELPK,
                        t.RRW_CODE,
                        t.RRW_DESC,
                        t.RRW_VALUE,
                        t.RRW_UPDATECOUNT,
                        t.RRW_LASTSAVED,
                        t.RRW_YES,
                        t.RRW_NO,
                        t.RRW_FINDING,
                        t.RRW_OK,
                        t.RRW_REPAIRSNEEDED,
                        t.RRW_RESOLUTION,
                        t.RRW_GOOD,
                        t.RRW_POOR,
                        t.RRW_ADJUSTED,
                        t.RRW_NONCONFORMITY,
                        t.RRW_SEVERITY,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5RELIABILITYRANKINGANSWER as  t
					 union
					        SELECT
                        th.RRW_PK,
                        th.RRW_LEVELPK,
                        th.RRW_CODE,
                        th.RRW_DESC,
                        th.RRW_VALUE,
                        th.RRW_UPDATECOUNT,
                        th.RRW_LASTSAVED,
                        th.RRW_YES,
                        th.RRW_NO,
                        th.RRW_FINDING,
                        th.RRW_OK,
                        th.RRW_REPAIRSNEEDED,
                        th.RRW_RESOLUTION,
                        th.RRW_GOOD,
                        th.RRW_POOR,
                        th.RRW_ADJUSTED,
                        th.RRW_NONCONFORMITY,
                        th.RRW_SEVERITY,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5RELIABILITYRANKINGANSWER as  th ;
                     