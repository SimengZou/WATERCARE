
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5FIRSTACT
                   as                       
                    SELECT
                        t.ACT_EVENT,
                        t.ACT_ACT,
                        t.ACT_START,
                        t.ACT_TRADE,
                        t.ACT_PERSONS,
                        t.ACT_DURATION,
                        t.ACT_EST,
                        t.ACT_REM,
                        t.ACT_TASK,
                        t.ACT_MATLIST,
                        t.ACT_MULTIPLETRADES,
                        t.ACT_RPC,
                        t.ACT_WAP,
                        t.ACT_TPF,
                        t.ACT_MANUFACT,
                        t.ACT_SYSLEVEL,
                        t.ACT_ASMLEVEL,
                        t.ACT_COMPLEVEL,
                        t.ACT_COMPLOCATION,
                        t.ACT_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5FIRSTACT as  t
					 union
					        SELECT
                        th.ACT_EVENT,
                        th.ACT_ACT,
                        th.ACT_START,
                        th.ACT_TRADE,
                        th.ACT_PERSONS,
                        th.ACT_DURATION,
                        th.ACT_EST,
                        th.ACT_REM,
                        th.ACT_TASK,
                        th.ACT_MATLIST,
                        th.ACT_MULTIPLETRADES,
                        th.ACT_RPC,
                        th.ACT_WAP,
                        th.ACT_TPF,
                        th.ACT_MANUFACT,
                        th.ACT_SYSLEVEL,
                        th.ACT_ASMLEVEL,
                        th.ACT_COMPLEVEL,
                        th.ACT_COMPLOCATION,
                        th.ACT_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5FIRSTACT as  th ;
                     