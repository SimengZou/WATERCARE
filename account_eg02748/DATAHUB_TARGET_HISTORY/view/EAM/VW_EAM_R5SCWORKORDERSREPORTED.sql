
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5SCWORKORDERSREPORTED
                   as                       
                    SELECT
                        t.CWR_ORG,
                        t.CWR_MRC,
                        t.CWR_DATE,
                        t.CWR_WOSREPORTED,
                        t.CWR_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5SCWORKORDERSREPORTED as  t
					 union
					        SELECT
                        th.CWR_ORG,
                        th.CWR_MRC,
                        th.CWR_DATE,
                        th.CWR_WOSREPORTED,
                        th.CWR_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5SCWORKORDERSREPORTED as  th ;
                     