
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5HOMEUSERS
                   as                       
                    SELECT
                        t.HMU_HOMCODE,
                        t.HMU_HOMTYPE,
                        t.HMU_USER,
                        t.HMU_SEQ,
                        t.HMU_AUTOFRESH,
                        t.HMU_UPDATECOUNT,
                        t.HMU_TAB,
                        t.HMU_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5HOMEUSERS as  t
					 union
					        SELECT
                        th.HMU_HOMCODE,
                        th.HMU_HOMTYPE,
                        th.HMU_USER,
                        th.HMU_SEQ,
                        th.HMU_AUTOFRESH,
                        th.HMU_UPDATECOUNT,
                        th.HMU_TAB,
                        th.HMU_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5HOMEUSERS as  th ;
                     