CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5BOILERTEXTS_ERROR AS SELECT src, 'EAM_R5BOILERTEXTS' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BOT_ADLEN), '0'), 38, 10) is null and 
                    src:BOT_ADLEN is not null) THEN
                    'BOT_ADLEN contains a non-numeric value : \'' || AS_VARCHAR(src:BOT_ADLEN) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BOT_CREATED), '1900-01-01')) is null and 
                    src:BOT_CREATED is not null) THEN
                    'BOT_CREATED contains a non-timestamp value : \'' || AS_VARCHAR(src:BOT_CREATED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BOT_LASTSAVED), '1900-01-01')) is null and 
                    src:BOT_LASTSAVED is not null) THEN
                    'BOT_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:BOT_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BOT_LENGTH), '0'), 38, 10) is null and 
                    src:BOT_LENGTH is not null) THEN
                    'BOT_LENGTH contains a non-numeric value : \'' || AS_VARCHAR(src:BOT_LENGTH) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BOT_LVCR), '0'), 38, 10) is null and 
                    src:BOT_LVCR is not null) THEN
                    'BOT_LVCR contains a non-numeric value : \'' || AS_VARCHAR(src:BOT_LVCR) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BOT_NUMBER), '0'), 38, 10) is null and 
                    src:BOT_NUMBER is not null) THEN
                    'BOT_NUMBER contains a non-numeric value : \'' || AS_VARCHAR(src:BOT_NUMBER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BOT_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:BOT_UPDATECOUNT is not null) THEN
                    'BOT_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:BOT_UPDATECOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BOT_UPDATED), '1900-01-01')) is null and 
                    src:BOT_UPDATED is not null) THEN
                    'BOT_UPDATED contains a non-timestamp value : \'' || AS_VARCHAR(src:BOT_UPDATED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BOT_XPOS), '0'), 38, 10) is null and 
                    src:BOT_XPOS is not null) THEN
                    'BOT_XPOS contains a non-numeric value : \'' || AS_VARCHAR(src:BOT_XPOS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BOT_YPOS), '0'), 38, 10) is null and 
                    src:BOT_YPOS is not null) THEN
                    'BOT_YPOS contains a non-numeric value : \'' || AS_VARCHAR(src:BOT_YPOS) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BOT_LASTSAVED), '1900-01-01')) is null and 
                src:BOT_LASTSAVED is not null) THEN
                'BOT_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:BOT_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:BOT_FUNCTION,
                src:BOT_LANG,
                src:BOT_NUMBER  ORDER BY 
            src:BOT_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5BOILERTEXTS as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BOT_ADLEN), '0'), 38, 10) is null and 
                    src:BOT_ADLEN is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BOT_CREATED), '1900-01-01')) is null and 
                    src:BOT_CREATED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BOT_LASTSAVED), '1900-01-01')) is null and 
                    src:BOT_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BOT_LENGTH), '0'), 38, 10) is null and 
                    src:BOT_LENGTH is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BOT_LVCR), '0'), 38, 10) is null and 
                    src:BOT_LVCR is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BOT_NUMBER), '0'), 38, 10) is null and 
                    src:BOT_NUMBER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BOT_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:BOT_UPDATECOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BOT_UPDATED), '1900-01-01')) is null and 
                    src:BOT_UPDATED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BOT_XPOS), '0'), 38, 10) is null and 
                    src:BOT_XPOS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BOT_YPOS), '0'), 38, 10) is null and 
                    src:BOT_YPOS is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BOT_LASTSAVED), '1900-01-01')) is null and 
                src:BOT_LASTSAVED is not null)