
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5OBJECTSURVEY
                   as                       
                    SELECT
                        t.OBS_OBJECT,
                        t.OBS_OBJECT_ORG,
                        t.OBS_TYPE,
                        t.OBS_LEVELPK,
                        t.OBS_ANSWERPK,
                        t.OBS_UPDATECOUNT,
                        t.OBS_VALUE,
                        t.OBS_CALCULATEDANSWER,
                        t.OBS_CALCULATEDVALUE,
                        t.OBS_DATELASTCALCULATED,
                        t.OBS_LASTSAVED,
                        t.OBS_WORKORDER,
                        t.OBS_OPERATORCHECKLIST,
                        t.OBS_DATEEFFECTIVE,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5OBJECTSURVEY as  t
					 union
					        SELECT
                        th.OBS_OBJECT,
                        th.OBS_OBJECT_ORG,
                        th.OBS_TYPE,
                        th.OBS_LEVELPK,
                        th.OBS_ANSWERPK,
                        th.OBS_UPDATECOUNT,
                        th.OBS_VALUE,
                        th.OBS_CALCULATEDANSWER,
                        th.OBS_CALCULATEDVALUE,
                        th.OBS_DATELASTCALCULATED,
                        th.OBS_LASTSAVED,
                        th.OBS_WORKORDER,
                        th.OBS_OPERATORCHECKLIST,
                        th.OBS_DATEEFFECTIVE,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5OBJECTSURVEY as  th ;
                     