
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5FUTUREDUPLICATES
                   as                       
                    SELECT
                        t.FDP_PPOPK,
                        t.FDP_SEQNO,
                        t.FDP_DATE,
                        t.FDP_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5FUTUREDUPLICATES as  t
					 union
					        SELECT
                        th.FDP_PPOPK,
                        th.FDP_SEQNO,
                        th.FDP_DATE,
                        th.FDP_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5FUTUREDUPLICATES as  th ;
                     