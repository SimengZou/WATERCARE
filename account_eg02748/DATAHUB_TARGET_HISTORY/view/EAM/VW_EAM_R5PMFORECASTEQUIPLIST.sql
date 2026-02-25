
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5PMFORECASTEQUIPLIST
                   as                       
                    SELECT
                        t.PFL_SESSIONID,
                        t.PFL_SELECT,
                        t.PFL_OBJECT,
                        t.PFL_OBJECT_ORG,
                        t.PFL_PARENT,
                        t.PFL_PARENT_ORG,
                        t.PFL_UPDATECOUNT,
                        t.PFL_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5PMFORECASTEQUIPLIST as  t
					 union
					        SELECT
                        th.PFL_SESSIONID,
                        th.PFL_SELECT,
                        th.PFL_OBJECT,
                        th.PFL_OBJECT_ORG,
                        th.PFL_PARENT,
                        th.PFL_PARENT_ORG,
                        th.PFL_UPDATECOUNT,
                        th.PFL_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5PMFORECASTEQUIPLIST as  th ;
                     