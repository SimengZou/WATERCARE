CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5TRACKINGPROMPTS_ERROR AS SELECT src, 'EAM_R5TRACKINGPROMPTS' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TKP_BRANCHGOTO), '0'), 38, 10) is null and 
                    src:TKP_BRANCHGOTO is not null) THEN
                    'TKP_BRANCHGOTO contains a non-numeric value : \'' || AS_VARCHAR(src:TKP_BRANCHGOTO) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TKP_LASTSAVED), '1900-01-01')) is null and 
                    src:TKP_LASTSAVED is not null) THEN
                    'TKP_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:TKP_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TKP_MAXSIZE), '0'), 38, 10) is null and 
                    src:TKP_MAXSIZE is not null) THEN
                    'TKP_MAXSIZE contains a non-numeric value : \'' || AS_VARCHAR(src:TKP_MAXSIZE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TKP_MINSIZE), '0'), 38, 10) is null and 
                    src:TKP_MINSIZE is not null) THEN
                    'TKP_MINSIZE contains a non-numeric value : \'' || AS_VARCHAR(src:TKP_MINSIZE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TKP_PROMPTSEQ), '0'), 38, 10) is null and 
                    src:TKP_PROMPTSEQ is not null) THEN
                    'TKP_PROMPTSEQ contains a non-numeric value : \'' || AS_VARCHAR(src:TKP_PROMPTSEQ) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TKP_RETURNTOPROMPT), '0'), 38, 10) is null and 
                    src:TKP_RETURNTOPROMPT is not null) THEN
                    'TKP_RETURNTOPROMPT contains a non-numeric value : \'' || AS_VARCHAR(src:TKP_RETURNTOPROMPT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TKP_TRANSGROUP), '0'), 38, 10) is null and 
                    src:TKP_TRANSGROUP is not null) THEN
                    'TKP_TRANSGROUP contains a non-numeric value : \'' || AS_VARCHAR(src:TKP_TRANSGROUP) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TKP_TRANSSEQ), '0'), 38, 10) is null and 
                    src:TKP_TRANSSEQ is not null) THEN
                    'TKP_TRANSSEQ contains a non-numeric value : \'' || AS_VARCHAR(src:TKP_TRANSSEQ) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TKP_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:TKP_UPDATECOUNT is not null) THEN
                    'TKP_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:TKP_UPDATECOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TKP_UPDATED), '1900-01-01')) is null and 
                    src:TKP_UPDATED is not null) THEN
                    'TKP_UPDATED contains a non-timestamp value : \'' || AS_VARCHAR(src:TKP_UPDATED) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TKP_LASTSAVED), '1900-01-01')) is null and 
                src:TKP_LASTSAVED is not null) THEN
                'TKP_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:TKP_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
                etl_load_datetime,
                etl_load_metadatafilename
                FROM 
                (
                select 
                    src,
                    etl_load_datetime,
                    etl_load_metadatafilename
                    from
                    (
                        SELECT
        
                            
            src,
            etl_load_datetime,
            etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
                                    
                src:TKP_TRANS,
                src:TKP_TRANSSEQ  ORDER BY 
            src:TKP_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5TRACKINGPROMPTS as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TKP_BRANCHGOTO), '0'), 38, 10) is null and 
                    src:TKP_BRANCHGOTO is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TKP_LASTSAVED), '1900-01-01')) is null and 
                    src:TKP_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TKP_MAXSIZE), '0'), 38, 10) is null and 
                    src:TKP_MAXSIZE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TKP_MINSIZE), '0'), 38, 10) is null and 
                    src:TKP_MINSIZE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TKP_PROMPTSEQ), '0'), 38, 10) is null and 
                    src:TKP_PROMPTSEQ is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TKP_RETURNTOPROMPT), '0'), 38, 10) is null and 
                    src:TKP_RETURNTOPROMPT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TKP_TRANSGROUP), '0'), 38, 10) is null and 
                    src:TKP_TRANSGROUP is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TKP_TRANSSEQ), '0'), 38, 10) is null and 
                    src:TKP_TRANSSEQ is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TKP_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:TKP_UPDATECOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TKP_UPDATED), '1900-01-01')) is null and 
                    src:TKP_UPDATED is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TKP_LASTSAVED), '1900-01-01')) is null and 
                src:TKP_LASTSAVED is not null)