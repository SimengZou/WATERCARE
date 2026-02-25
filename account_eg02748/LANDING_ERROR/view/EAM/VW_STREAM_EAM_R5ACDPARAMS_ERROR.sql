CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5ACDPARAMS_ERROR AS SELECT src, 'EAM_R5ACDPARAMS' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADP_LASTSAVED), '1900-01-01')) is null and 
                    src:ADP_LASTSAVED is not null) THEN
                    'ADP_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:ADP_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADP_LENGTH), '0'), 38, 10) is null and 
                    src:ADP_LENGTH is not null) THEN
                    'ADP_LENGTH contains a non-numeric value : \'' || AS_VARCHAR(src:ADP_LENGTH) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADP_PROMPTX), '0'), 38, 10) is null and 
                    src:ADP_PROMPTX is not null) THEN
                    'ADP_PROMPTX contains a non-numeric value : \'' || AS_VARCHAR(src:ADP_PROMPTX) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADP_PROMPTY), '0'), 38, 10) is null and 
                    src:ADP_PROMPTY is not null) THEN
                    'ADP_PROMPTY contains a non-numeric value : \'' || AS_VARCHAR(src:ADP_PROMPTY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADP_SEGMENTX), '0'), 38, 10) is null and 
                    src:ADP_SEGMENTX is not null) THEN
                    'ADP_SEGMENTX contains a non-numeric value : \'' || AS_VARCHAR(src:ADP_SEGMENTX) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADP_SEGMENTY), '0'), 38, 10) is null and 
                    src:ADP_SEGMENTY is not null) THEN
                    'ADP_SEGMENTY contains a non-numeric value : \'' || AS_VARCHAR(src:ADP_SEGMENTY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADP_SEQ), '0'), 38, 10) is null and 
                    src:ADP_SEQ is not null) THEN
                    'ADP_SEQ contains a non-numeric value : \'' || AS_VARCHAR(src:ADP_SEQ) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADP_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:ADP_UPDATECOUNT is not null) THEN
                    'ADP_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:ADP_UPDATECOUNT) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADP_LASTSAVED), '1900-01-01')) is null and 
                src:ADP_LASTSAVED is not null) THEN
                'ADP_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:ADP_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:ADP_SEGMENT  ORDER BY 
            src:ADP_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5ACDPARAMS as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADP_LASTSAVED), '1900-01-01')) is null and 
                    src:ADP_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADP_LENGTH), '0'), 38, 10) is null and 
                    src:ADP_LENGTH is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADP_PROMPTX), '0'), 38, 10) is null and 
                    src:ADP_PROMPTX is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADP_PROMPTY), '0'), 38, 10) is null and 
                    src:ADP_PROMPTY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADP_SEGMENTX), '0'), 38, 10) is null and 
                    src:ADP_SEGMENTX is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADP_SEGMENTY), '0'), 38, 10) is null and 
                    src:ADP_SEGMENTY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADP_SEQ), '0'), 38, 10) is null and 
                    src:ADP_SEQ is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADP_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:ADP_UPDATECOUNT is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADP_LASTSAVED), '1900-01-01')) is null and 
                src:ADP_LASTSAVED is not null)