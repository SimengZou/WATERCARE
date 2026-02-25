
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5SEQINSTALLMAPPING
                   as                       
                    SELECT
                        t.SIM_SEQUENCE,
                        t.SIM_INSTALLPARAMETER,
                        t.SIM_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5SEQINSTALLMAPPING as  t
					 union
					        SELECT
                        th.SIM_SEQUENCE,
                        th.SIM_INSTALLPARAMETER,
                        th.SIM_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5SEQINSTALLMAPPING as  th ;
                     