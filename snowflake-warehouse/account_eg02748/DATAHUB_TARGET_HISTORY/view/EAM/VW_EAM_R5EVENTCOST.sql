
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5EVENTCOST
                   as                       
                    SELECT
                        t.EVO_EVENT,
                        t.EVO_RECALCCOST,
                        t.EVO_COSTCALCULATED,
                        t.EVO_LABOR,
                        t.EVO_MATERIAL,
                        t.EVO_TOOL,
                        t.EVO_TOTAL,
                        t.EVO_HOURS,
                        t.EVO_UPDATED,
                        t.EVO_LASTSAVED,
                        t.EVO_OWNLABOR,
                        t.EVO_HIREDLABOR,
                        t.EVO_SERVICESLABOR,
                        t.EVO_STOCKMATERIAL,
                        t.EVO_DIRECTMATERIAL,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5EVENTCOST as  t
					 union
					        SELECT
                        th.EVO_EVENT,
                        th.EVO_RECALCCOST,
                        th.EVO_COSTCALCULATED,
                        th.EVO_LABOR,
                        th.EVO_MATERIAL,
                        th.EVO_TOOL,
                        th.EVO_TOTAL,
                        th.EVO_HOURS,
                        th.EVO_UPDATED,
                        th.EVO_LASTSAVED,
                        th.EVO_OWNLABOR,
                        th.EVO_HIREDLABOR,
                        th.EVO_SERVICESLABOR,
                        th.EVO_STOCKMATERIAL,
                        th.EVO_DIRECTMATERIAL,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5EVENTCOST as  th ;
                     