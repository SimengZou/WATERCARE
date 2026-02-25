
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5PMFORECASTPREVIEW
                   as                       
                    SELECT
                        t.PFV_SESSIONID,
                        t.PFV_SELECT,
                        t.PFV_OBJECT,
                        t.PFV_OBJECT_ORG,
                        t.PFV_PARENT,
                        t.PFV_PARENT_ORG,
                        t.PFV_UPDATECOUNT,
                        t.PFV_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5PMFORECASTPREVIEW as  t
					 union
					        SELECT
                        th.PFV_SESSIONID,
                        th.PFV_SELECT,
                        th.PFV_OBJECT,
                        th.PFV_OBJECT_ORG,
                        th.PFV_PARENT,
                        th.PFV_PARENT_ORG,
                        th.PFV_UPDATECOUNT,
                        th.PFV_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5PMFORECASTPREVIEW as  th ;
                     