
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5FUNCVIEWMAP
                   as                       
                    SELECT
                        t.FVM_FUNCVIEW,
                        t.FVM_GROUP,
                        t.FVM_FUNCTION,
                        t.FVM_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5FUNCVIEWMAP as  t
					 union
					        SELECT
                        th.FVM_FUNCVIEW,
                        th.FVM_GROUP,
                        th.FVM_FUNCTION,
                        th.FVM_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5FUNCVIEWMAP as  th ;
                     