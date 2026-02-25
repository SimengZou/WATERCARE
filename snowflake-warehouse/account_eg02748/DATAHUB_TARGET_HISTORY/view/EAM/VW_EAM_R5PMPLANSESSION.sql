
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5PMPLANSESSION
                   as                       
                    SELECT
                        t.PPS_PK,
                        t.PPS_SESSIONID,
                        t.PPS_PPOPK,
                        t.PPS_PMREVISION,
                        t.PPS_DUEWEEKOFMONTH,
                        t.PPS_DUEDAYOFWEEK,
                        t.PPS_LOCKED,
                        t.PPS_DUEDATE,
                        t.PPS_NESTINGREF,
                        t.PPS_IGNOREFREQWARNING,
                        t.PPS_IGNORERANGEWARNING,
                        t.PPS_UPDATECOUNT,
                        t.PPS_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5PMPLANSESSION as  t
					 union
					        SELECT
                        th.PPS_PK,
                        th.PPS_SESSIONID,
                        th.PPS_PPOPK,
                        th.PPS_PMREVISION,
                        th.PPS_DUEWEEKOFMONTH,
                        th.PPS_DUEDAYOFWEEK,
                        th.PPS_LOCKED,
                        th.PPS_DUEDATE,
                        th.PPS_NESTINGREF,
                        th.PPS_IGNOREFREQWARNING,
                        th.PPS_IGNORERANGEWARNING,
                        th.PPS_UPDATECOUNT,
                        th.PPS_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5PMPLANSESSION as  th ;
                     