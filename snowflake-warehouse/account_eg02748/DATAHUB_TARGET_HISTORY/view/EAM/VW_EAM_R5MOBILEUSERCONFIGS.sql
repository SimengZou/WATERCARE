
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5MOBILEUSERCONFIGS
                   as                       
                    SELECT
                        t.MUC_USER,
                        t.MUC_GROUP,
                        t.MUC_CODE,
                        t.MUC_DESK,
                        t.MUC_RCODE,
                        t.MUC_COMPTYPE,
                        t.MUC_CREATED,
                        t.MUC_UPDATED,
                        t.MUC_UPDATECOUNT,
                        t.MUC_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5MOBILEUSERCONFIGS as  t
					 union
					        SELECT
                        th.MUC_USER,
                        th.MUC_GROUP,
                        th.MUC_CODE,
                        th.MUC_DESK,
                        th.MUC_RCODE,
                        th.MUC_COMPTYPE,
                        th.MUC_CREATED,
                        th.MUC_UPDATED,
                        th.MUC_UPDATECOUNT,
                        th.MUC_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5MOBILEUSERCONFIGS as  th ;
                     