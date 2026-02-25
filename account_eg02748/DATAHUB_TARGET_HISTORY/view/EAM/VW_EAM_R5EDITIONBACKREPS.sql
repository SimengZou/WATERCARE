
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5EDITIONBACKREPS
                   as                       
                    SELECT
                        t.EBR_FUNCTION,
                        t.EBR_MEFLAG,
                        t.EBR_REPORT,
                        t.EBR_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5EDITIONBACKREPS as  t
					 union
					        SELECT
                        th.EBR_FUNCTION,
                        th.EBR_MEFLAG,
                        th.EBR_REPORT,
                        th.EBR_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5EDITIONBACKREPS as  th ;
                     