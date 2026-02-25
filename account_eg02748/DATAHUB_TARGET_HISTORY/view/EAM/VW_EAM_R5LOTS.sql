
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5LOTS
                   as                       
                    SELECT
                        t.LOT_CODE,
                        t.LOT_DESC,
                        t.LOT_CLASS,
                        t.LOT_EXPIRED,
                        t.LOT_MANLOT,
                        t.LOT_ORG,
                        t.LOT_CLASS_ORG,
                        t.LOT_UPDATECOUNT,
                        t.LOT_BUILDKITTRANS,
                        t.LOT_BREAKUPKITTRANS,
                        t.LOT_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5LOTS as  t
					 union
					        SELECT
                        th.LOT_CODE,
                        th.LOT_DESC,
                        th.LOT_CLASS,
                        th.LOT_EXPIRED,
                        th.LOT_MANLOT,
                        th.LOT_ORG,
                        th.LOT_CLASS_ORG,
                        th.LOT_UPDATECOUNT,
                        th.LOT_BUILDKITTRANS,
                        th.LOT_BREAKUPKITTRANS,
                        th.LOT_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5LOTS as  th ;
                     