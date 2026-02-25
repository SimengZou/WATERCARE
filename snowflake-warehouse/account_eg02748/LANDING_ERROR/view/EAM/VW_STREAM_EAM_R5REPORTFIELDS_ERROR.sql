CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5REPORTFIELDS_ERROR AS SELECT src, 'EAM_R5REPORTFIELDS' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RFL_BOTNUMBER), '0'), 38, 10) is null and 
                    src:RFL_BOTNUMBER is not null) THEN
                    'RFL_BOTNUMBER contains a non-numeric value : \'' || AS_VARCHAR(src:RFL_BOTNUMBER) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:RFL_LASTSAVED), '1900-01-01')) is null and 
                    src:RFL_LASTSAVED is not null) THEN
                    'RFL_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:RFL_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RFL_LINE), '0'), 38, 10) is null and 
                    src:RFL_LINE is not null) THEN
                    'RFL_LINE contains a non-numeric value : \'' || AS_VARCHAR(src:RFL_LINE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RFL_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:RFL_UPDATECOUNT is not null) THEN
                    'RFL_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:RFL_UPDATECOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:RFL_UPDATED), '1900-01-01')) is null and 
                    src:RFL_UPDATED is not null) THEN
                    'RFL_UPDATED contains a non-timestamp value : \'' || AS_VARCHAR(src:RFL_UPDATED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RFL_WIDTH), '0'), 38, 10) is null and 
                    src:RFL_WIDTH is not null) THEN
                    'RFL_WIDTH contains a non-numeric value : \'' || AS_VARCHAR(src:RFL_WIDTH) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:RFL_LASTSAVED), '1900-01-01')) is null and 
                src:RFL_LASTSAVED is not null) THEN
                'RFL_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:RFL_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:RFL_FUNCTION,
                src:RFL_LINE  ORDER BY 
            src:RFL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5REPORTFIELDS as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RFL_BOTNUMBER), '0'), 38, 10) is null and 
                    src:RFL_BOTNUMBER is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:RFL_LASTSAVED), '1900-01-01')) is null and 
                    src:RFL_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RFL_LINE), '0'), 38, 10) is null and 
                    src:RFL_LINE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RFL_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:RFL_UPDATECOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:RFL_UPDATED), '1900-01-01')) is null and 
                    src:RFL_UPDATED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RFL_WIDTH), '0'), 38, 10) is null and 
                    src:RFL_WIDTH is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:RFL_LASTSAVED), '1900-01-01')) is null and 
                src:RFL_LASTSAVED is not null)