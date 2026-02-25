
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5EVENTOBJECTS
                   as                       
                    SELECT
                        t.EOB_EVENT,
                        t.EOB_OBTYPE,
                        t.EOB_OBRTYPE,
                        t.EOB_OBJECT,
                        t.EOB_LEVEL,
                        t.EOB_ROLLUP,
                        t.EOB_DOWNTIME,
                        t.EOB_OBJECT_ORG,
                        t.EOB_UPDATECOUNT,
                        t.EOB_FROMPOINT,
                        t.EOB_TOPOINT,
                        t.EOB_WEIGHTPERCENTAGE,
                        t.EOB_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5EVENTOBJECTS as  t
					 union
					        SELECT
                        th.EOB_EVENT,
                        th.EOB_OBTYPE,
                        th.EOB_OBRTYPE,
                        th.EOB_OBJECT,
                        th.EOB_LEVEL,
                        th.EOB_ROLLUP,
                        th.EOB_DOWNTIME,
                        th.EOB_OBJECT_ORG,
                        th.EOB_UPDATECOUNT,
                        th.EOB_FROMPOINT,
                        th.EOB_TOPOINT,
                        th.EOB_WEIGHTPERCENTAGE,
                        th.EOB_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5EVENTOBJECTS as  th ;
                     