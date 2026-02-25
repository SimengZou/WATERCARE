CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5REPORTSUBTOTAL_ERROR AS SELECT src, 'EAM_R5REPORTSUBTOTAL' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RST_BOTNUMBER), '0'), 38, 10) is null and 
                    src:RST_BOTNUMBER is not null) THEN
                    'RST_BOTNUMBER contains a non-numeric value : \'' || AS_VARCHAR(src:RST_BOTNUMBER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RST_GROUPLINE), '0'), 38, 10) is null and 
                    src:RST_GROUPLINE is not null) THEN
                    'RST_GROUPLINE contains a non-numeric value : \'' || AS_VARCHAR(src:RST_GROUPLINE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:RST_LASTSAVED), '1900-01-01')) is null and 
                    src:RST_LASTSAVED is not null) THEN
                    'RST_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:RST_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RST_LINE), '0'), 38, 10) is null and 
                    src:RST_LINE is not null) THEN
                    'RST_LINE contains a non-numeric value : \'' || AS_VARCHAR(src:RST_LINE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RST_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:RST_UPDATECOUNT is not null) THEN
                    'RST_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:RST_UPDATECOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:RST_UPDATED), '1900-01-01')) is null and 
                    src:RST_UPDATED is not null) THEN
                    'RST_UPDATED contains a non-timestamp value : \'' || AS_VARCHAR(src:RST_UPDATED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RST_WIDTH), '0'), 38, 10) is null and 
                    src:RST_WIDTH is not null) THEN
                    'RST_WIDTH contains a non-numeric value : \'' || AS_VARCHAR(src:RST_WIDTH) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:RST_LASTSAVED), '1900-01-01')) is null and 
                src:RST_LASTSAVED is not null) THEN
                'RST_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:RST_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:RST_FUNCTION,
                src:RST_GROUPLINE,
                src:RST_LINE  ORDER BY 
            src:RST_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5REPORTSUBTOTAL as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RST_BOTNUMBER), '0'), 38, 10) is null and 
                    src:RST_BOTNUMBER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RST_GROUPLINE), '0'), 38, 10) is null and 
                    src:RST_GROUPLINE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:RST_LASTSAVED), '1900-01-01')) is null and 
                    src:RST_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RST_LINE), '0'), 38, 10) is null and 
                    src:RST_LINE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RST_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:RST_UPDATECOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:RST_UPDATED), '1900-01-01')) is null and 
                    src:RST_UPDATED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RST_WIDTH), '0'), 38, 10) is null and 
                    src:RST_WIDTH is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:RST_LASTSAVED), '1900-01-01')) is null and 
                src:RST_LASTSAVED is not null)