
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5ALERTHISTORY
                   as                       
                    SELECT
                        t.ALH_ALERT,
                        t.ALH_RSTATUS,
                        t.ALH_RTYPE,
                        t.ALH_ENTITYCODE,
                        t.ALH_ENTITYORG,
                        t.ALH_TEMPLATE,
                        t.ALH_RESULTCODE,
                        t.ALH_RESULTORG,
                        t.ALH_ERRORMESSAGE,
                        t.ALH_CREATED,
                        t.ALH_UPDATECOUNT,
                        t.ALH_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5ALERTHISTORY as  t
					 union
					        SELECT
                        th.ALH_ALERT,
                        th.ALH_RSTATUS,
                        th.ALH_RTYPE,
                        th.ALH_ENTITYCODE,
                        th.ALH_ENTITYORG,
                        th.ALH_TEMPLATE,
                        th.ALH_RESULTCODE,
                        th.ALH_RESULTORG,
                        th.ALH_ERRORMESSAGE,
                        th.ALH_CREATED,
                        th.ALH_UPDATECOUNT,
                        th.ALH_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5ALERTHISTORY as  th ;
                     