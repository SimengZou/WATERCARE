
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5IESINTERESTCENTERS
                   as                       
                    SELECT
                        t.ENI_CODE,
                        t.ENI_DESC,
                        t.ENI_CATEGORY,
                        t.ENI_NOTUSED,
                        t.ENI_UPDATECOUNT,
                        t.ENI_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5IESINTERESTCENTERS as  t
					 union
					        SELECT
                        th.ENI_CODE,
                        th.ENI_DESC,
                        th.ENI_CATEGORY,
                        th.ENI_NOTUSED,
                        th.ENI_UPDATECOUNT,
                        th.ENI_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5IESINTERESTCENTERS as  th ;
                     