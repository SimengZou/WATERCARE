
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5SCHEDGROUPS
                   as                       
                    SELECT
                        t.SCG_CODE,
                        t.SCG_DESC,
                        t.SCG_CLASS,
                        t.SCG_ORG,
                        t.SCG_CLASS_ORG,
                        t.SCG_UPDATECOUNT,
                        t.SCG_NOTUSED,
                        t.SCG_LASTSAVED,
                        t.SCG_PROFILEPICTURE,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5SCHEDGROUPS as  t
					 union
					        SELECT
                        th.SCG_CODE,
                        th.SCG_DESC,
                        th.SCG_CLASS,
                        th.SCG_ORG,
                        th.SCG_CLASS_ORG,
                        th.SCG_UPDATECOUNT,
                        th.SCG_NOTUSED,
                        th.SCG_LASTSAVED,
                        th.SCG_PROFILEPICTURE,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5SCHEDGROUPS as  th ;
                     