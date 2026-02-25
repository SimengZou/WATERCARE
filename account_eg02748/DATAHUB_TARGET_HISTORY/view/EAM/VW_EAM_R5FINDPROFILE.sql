
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5FINDPROFILE
                   as                       
                    SELECT
                        t.FPF_CODE,
                        t.FPF_PROFILE,
                        t.FPF_PROFILE_ORG,
                        t.FPF_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5FINDPROFILE as  t
					 union
					        SELECT
                        th.FPF_CODE,
                        th.FPF_PROFILE,
                        th.FPF_PROFILE_ORG,
                        th.FPF_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5FINDPROFILE as  th ;
                     