
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5WSPRETURNPROMPTS
                   as                       
                    SELECT
                        t.WPR_WSPCODE,
                        t.WPR_SOURCESEQ,
                        t.WPR_TARGETSEQ,
                        t.WPR_QUERYCODE,
                        t.WPR_UPDATED,
                        t.WPR_UPDATECOUNT,
                        t.WPR_MOBILEQUERYCODE,
                        t.WPR_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5WSPRETURNPROMPTS as  t
					 union
					        SELECT
                        th.WPR_WSPCODE,
                        th.WPR_SOURCESEQ,
                        th.WPR_TARGETSEQ,
                        th.WPR_QUERYCODE,
                        th.WPR_UPDATED,
                        th.WPR_UPDATECOUNT,
                        th.WPR_MOBILEQUERYCODE,
                        th.WPR_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5WSPRETURNPROMPTS as  th ;
                     