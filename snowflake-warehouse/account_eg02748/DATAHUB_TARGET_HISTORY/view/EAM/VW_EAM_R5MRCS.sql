
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5MRCS
                   as                       
                    SELECT
                        t.MRC_CODE,
                        t.MRC_DESC,
                        t.MRC_CLASS,
                        t.MRC_STORE,
                        t.MRC_DFLTSCREENER,
                        t.MRC_SCHEDGROUP,
                        t.MRC_CAPACITY,
                        t.MRC_ORG,
                        t.MRC_CLASS_ORG,
                        t.MRC_UPDATECOUNT,
                        t.MRC_CREATED,
                        t.MRC_UPDATED,
                        t.MRC_NOTUSED,
                        t.MRC_SEGMENTVALUE,
                        t.MRC_LASTSAVED,
                        t.MRC_DEPOT,
                        t.MRC_DEPOT_ORG,
                        t.MRC_AVAILABLEFORCUS,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5MRCS as  t
					 union
					        SELECT
                        th.MRC_CODE,
                        th.MRC_DESC,
                        th.MRC_CLASS,
                        th.MRC_STORE,
                        th.MRC_DFLTSCREENER,
                        th.MRC_SCHEDGROUP,
                        th.MRC_CAPACITY,
                        th.MRC_ORG,
                        th.MRC_CLASS_ORG,
                        th.MRC_UPDATECOUNT,
                        th.MRC_CREATED,
                        th.MRC_UPDATED,
                        th.MRC_NOTUSED,
                        th.MRC_SEGMENTVALUE,
                        th.MRC_LASTSAVED,
                        th.MRC_DEPOT,
                        th.MRC_DEPOT_ORG,
                        th.MRC_AVAILABLEFORCUS,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5MRCS as  th ;
                     