
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5CONNLOCK
                   as                       
                    SELECT
                        t.CLK_OPERATIONID,
                        t.CLK_SESSIONID,
                        t.CLK_SESSIONSTART,
                        t.CLK_SESSIONUPDATE,
                        t.CLK_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5CONNLOCK as  t
					 union
					        SELECT
                        th.CLK_OPERATIONID,
                        th.CLK_SESSIONID,
                        th.CLK_SESSIONSTART,
                        th.CLK_SESSIONUPDATE,
                        th.CLK_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5CONNLOCK as  th ;
                     