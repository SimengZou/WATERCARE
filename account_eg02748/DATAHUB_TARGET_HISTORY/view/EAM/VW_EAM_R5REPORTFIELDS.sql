
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5REPORTFIELDS
                   as                       
                    SELECT
                        t.RFL_FUNCTION,
                        t.RFL_LINE,
                        t.RFL_BOTNUMBER,
                        t.RFL_FIELD,
                        t.RFL_DATATYPE,
                        t.RFL_SHOWFIELD,
                        t.RFL_WIDTH,
                        t.RFL_UPDATECOUNT,
                        t.RFL_UPDATED,
                        t.RFL_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5REPORTFIELDS as  t
					 union
					        SELECT
                        th.RFL_FUNCTION,
                        th.RFL_LINE,
                        th.RFL_BOTNUMBER,
                        th.RFL_FIELD,
                        th.RFL_DATATYPE,
                        th.RFL_SHOWFIELD,
                        th.RFL_WIDTH,
                        th.RFL_UPDATECOUNT,
                        th.RFL_UPDATED,
                        th.RFL_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5REPORTFIELDS as  th ;
                     