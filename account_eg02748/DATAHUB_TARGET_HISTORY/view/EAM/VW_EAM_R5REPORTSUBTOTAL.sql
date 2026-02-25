
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5REPORTSUBTOTAL
                   as                       
                    SELECT
                        t.RST_FUNCTION,
                        t.RST_GROUPLINE,
                        t.RST_LINE,
                        t.RST_BOTNUMBER,
                        t.RST_FIELD,
                        t.RST_DATATYPE,
                        t.RST_WIDTH,
                        t.RST_UPDATECOUNT,
                        t.RST_UPDATED,
                        t.RST_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5REPORTSUBTOTAL as  t
					 union
					        SELECT
                        th.RST_FUNCTION,
                        th.RST_GROUPLINE,
                        th.RST_LINE,
                        th.RST_BOTNUMBER,
                        th.RST_FIELD,
                        th.RST_DATATYPE,
                        th.RST_WIDTH,
                        th.RST_UPDATECOUNT,
                        th.RST_UPDATED,
                        th.RST_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5REPORTSUBTOTAL as  th ;
                     