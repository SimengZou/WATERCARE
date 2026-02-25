
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5FUTUREEVENTS
                   as                       
                    SELECT
                        t.FUT_EVENT,
                        t.FUT_SEQNO,
                        t.FUT_DATE,
                        t.FUT_UPDATECOUNT,
                        t.FUT_MP_SEQ,
                        t.FUT_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5FUTUREEVENTS as  t
					 union
					        SELECT
                        th.FUT_EVENT,
                        th.FUT_SEQNO,
                        th.FUT_DATE,
                        th.FUT_UPDATECOUNT,
                        th.FUT_MP_SEQ,
                        th.FUT_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5FUTUREEVENTS as  th ;
                     