
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5POINTTYPES
                   as                       
                    SELECT
                        t.PTP_CODE,
                        t.PTP_DESC,
                        t.PTP_CLASS,
                        t.PTP_CLASS_ORG,
                        t.PTP_UPDATECOUNT,
                        t.PTP_CREATED,
                        t.PTP_UPDATED,
                        t.PTP_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5POINTTYPES as  t
					 union
					        SELECT
                        th.PTP_CODE,
                        th.PTP_DESC,
                        th.PTP_CLASS,
                        th.PTP_CLASS_ORG,
                        th.PTP_UPDATECOUNT,
                        th.PTP_CREATED,
                        th.PTP_UPDATED,
                        th.PTP_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5POINTTYPES as  th ;
                     