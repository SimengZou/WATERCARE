
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5IPFUNCTIONS
                   as                       
                    SELECT
                        t.IPF_CODE,
                        t.IPF_DESC,
                        t.IPF_FIELD,
                        t.IPF_UPDATECOUNT,
                        t.IPF_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5IPFUNCTIONS as  t
					 union
					        SELECT
                        th.IPF_CODE,
                        th.IPF_DESC,
                        th.IPF_FIELD,
                        th.IPF_UPDATECOUNT,
                        th.IPF_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5IPFUNCTIONS as  th ;
                     