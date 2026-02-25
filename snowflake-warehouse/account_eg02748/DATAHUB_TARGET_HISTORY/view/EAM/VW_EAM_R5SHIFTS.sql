
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5SHIFTS
                   as                       
                    SELECT
                        t.SHF_CODE,
                        t.SHF_DESC,
                        t.SHF_CLASS,
                        t.SHF_LENGTH,
                        t.SHF_START,
                        t.SHF_ORG,
                        t.SHF_CLASS_ORG,
                        t.SHF_UPDATECOUNT,
                        t.SHF_UPDATED,
                        t.SHF_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5SHIFTS as  t
					 union
					        SELECT
                        th.SHF_CODE,
                        th.SHF_DESC,
                        th.SHF_CLASS,
                        th.SHF_LENGTH,
                        th.SHF_START,
                        th.SHF_ORG,
                        th.SHF_CLASS_ORG,
                        th.SHF_UPDATECOUNT,
                        th.SHF_UPDATED,
                        th.SHF_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5SHIFTS as  th ;
                     