
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5ACDPARAMS
                   as                       
                    SELECT
                        t.ADP_SEGMENT,
                        t.ADP_LENGTH,
                        t.ADP_DATATYPE,
                        t.ADP_PROMPT,
                        t.ADP_PROMPTX,
                        t.ADP_PROMPTY,
                        t.ADP_SEGMENTX,
                        t.ADP_SEGMENTY,
                        t.ADP_SEQ,
                        t.ADP_REQUIRED,
                        t.ADP_DEFAULT,
                        t.ADP_HINT,
                        t.ADP_LOVSQL,
                        t.ADP_DESCSQL,
                        t.ADP_UPDATECOUNT,
                        t.ADP_QUERY,
                        t.ADP_VALUELOOKUP,
                        t.ADP_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5ACDPARAMS as  t
					 union
					        SELECT
                        th.ADP_SEGMENT,
                        th.ADP_LENGTH,
                        th.ADP_DATATYPE,
                        th.ADP_PROMPT,
                        th.ADP_PROMPTX,
                        th.ADP_PROMPTY,
                        th.ADP_SEGMENTX,
                        th.ADP_SEGMENTY,
                        th.ADP_SEQ,
                        th.ADP_REQUIRED,
                        th.ADP_DEFAULT,
                        th.ADP_HINT,
                        th.ADP_LOVSQL,
                        th.ADP_DESCSQL,
                        th.ADP_UPDATECOUNT,
                        th.ADP_QUERY,
                        th.ADP_VALUELOOKUP,
                        th.ADP_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5ACDPARAMS as  th ;
                     