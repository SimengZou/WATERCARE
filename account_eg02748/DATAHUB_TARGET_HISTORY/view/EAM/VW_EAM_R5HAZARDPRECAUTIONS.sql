
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5HAZARDPRECAUTIONS
                   as                       
                    SELECT
                        t.HZP_HAZARD,
                        t.HZP_HAZARD_ORG,
                        t.HZP_REVISION,
                        t.HZP_PRECAUTION,
                        t.HZP_PRECAUTION_ORG,
                        t.HZP_SEQUENCE,
                        t.HZP_UPDATECOUNT,
                        t.HZP_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5HAZARDPRECAUTIONS as  t
					 union
					        SELECT
                        th.HZP_HAZARD,
                        th.HZP_HAZARD_ORG,
                        th.HZP_REVISION,
                        th.HZP_PRECAUTION,
                        th.HZP_PRECAUTION_ORG,
                        th.HZP_SEQUENCE,
                        th.HZP_UPDATECOUNT,
                        th.HZP_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5HAZARDPRECAUTIONS as  th ;
                     