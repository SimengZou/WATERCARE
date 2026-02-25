
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5REPORTFUNCTIONS
                   as                       
                    SELECT
                        t.RFN_FUNCTION,
                        t.RFN_FIELDORDER,
                        t.RFN_GROUPBY,
                        t.RFN_ORDERBY,
                        t.RFN_ORDERTYPE,
                        t.RFN_FROMCLAUSE,
                        t.RFN_WHERECLAUSE1,
                        t.RFN_WHERECLAUSE2,
                        t.RFN_UPDATECOUNT,
                        t.RFN_UPDATED,
                        t.RFN_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5REPORTFUNCTIONS as  t
					 union
					        SELECT
                        th.RFN_FUNCTION,
                        th.RFN_FIELDORDER,
                        th.RFN_GROUPBY,
                        th.RFN_ORDERBY,
                        th.RFN_ORDERTYPE,
                        th.RFN_FROMCLAUSE,
                        th.RFN_WHERECLAUSE1,
                        th.RFN_WHERECLAUSE2,
                        th.RFN_UPDATECOUNT,
                        th.RFN_UPDATED,
                        th.RFN_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5REPORTFUNCTIONS as  th ;
                     