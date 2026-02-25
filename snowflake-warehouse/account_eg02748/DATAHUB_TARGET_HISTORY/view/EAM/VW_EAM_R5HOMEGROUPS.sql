
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5HOMEGROUPS
                   as                       
                    SELECT
                        t.HMG_HOMCODE,
                        t.HMG_HOMTYPE,
                        t.HMG_GROUP,
                        t.HMG_UPDATECOUNT,
                        t.HMG_SEQ,
                        t.HMG_AUTOFRESH,
                        t.HMG_TAB,
                        t.HMG_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5HOMEGROUPS as  t
					 union
					        SELECT
                        th.HMG_HOMCODE,
                        th.HMG_HOMTYPE,
                        th.HMG_GROUP,
                        th.HMG_UPDATECOUNT,
                        th.HMG_SEQ,
                        th.HMG_AUTOFRESH,
                        th.HMG_TAB,
                        th.HMG_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5HOMEGROUPS as  th ;
                     