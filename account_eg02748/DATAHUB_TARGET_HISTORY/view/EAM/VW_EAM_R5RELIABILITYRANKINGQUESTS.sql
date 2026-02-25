
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5RELIABILITYRANKINGQUESTS
                   as                       
                    SELECT
                        t.RRQ_LEVELPK,
                        t.RRQ_LANG,
                        t.RRQ_QUESTION,
                        t.RRQ_TRANS,
                        t.RRQ_UPDATECOUNT,
                        t.RRQ_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5RELIABILITYRANKINGQUESTS as  t
					 union
					        SELECT
                        th.RRQ_LEVELPK,
                        th.RRQ_LANG,
                        th.RRQ_QUESTION,
                        th.RRQ_TRANS,
                        th.RRQ_UPDATECOUNT,
                        th.RRQ_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5RELIABILITYRANKINGQUESTS as  th ;
                     