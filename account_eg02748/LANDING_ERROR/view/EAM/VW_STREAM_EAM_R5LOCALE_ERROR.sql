CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5LOCALE_ERROR AS SELECT src, 'EAM_R5LOCALE' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LOC_FRACTDIGITS), '0'), 38, 10) is null and 
                    src:LOC_FRACTDIGITS is not null) THEN
                    'LOC_FRACTDIGITS contains a non-numeric value : \'' || AS_VARCHAR(src:LOC_FRACTDIGITS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LOC_GRPDIGITS), '0'), 38, 10) is null and 
                    src:LOC_GRPDIGITS is not null) THEN
                    'LOC_GRPDIGITS contains a non-numeric value : \'' || AS_VARCHAR(src:LOC_GRPDIGITS) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:LOC_LASTSAVED), '1900-01-01')) is null and 
                    src:LOC_LASTSAVED is not null) THEN
                    'LOC_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:LOC_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LOC_MONFRACT), '0'), 38, 10) is null and 
                    src:LOC_MONFRACT is not null) THEN
                    'LOC_MONFRACT contains a non-numeric value : \'' || AS_VARCHAR(src:LOC_MONFRACT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LOC_MONTGRPDIGITS), '0'), 38, 10) is null and 
                    src:LOC_MONTGRPDIGITS is not null) THEN
                    'LOC_MONTGRPDIGITS contains a non-numeric value : \'' || AS_VARCHAR(src:LOC_MONTGRPDIGITS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LOC_STARTDAY), '0'), 38, 10) is null and 
                    src:LOC_STARTDAY is not null) THEN
                    'LOC_STARTDAY contains a non-numeric value : \'' || AS_VARCHAR(src:LOC_STARTDAY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LOC_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:LOC_UPDATECOUNT is not null) THEN
                    'LOC_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:LOC_UPDATECOUNT) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:LOC_LASTSAVED), '1900-01-01')) is null and 
                src:LOC_LASTSAVED is not null) THEN
                'LOC_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:LOC_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:LOC_CODE  ORDER BY 
            src:LOC_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5LOCALE as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LOC_FRACTDIGITS), '0'), 38, 10) is null and 
                    src:LOC_FRACTDIGITS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LOC_GRPDIGITS), '0'), 38, 10) is null and 
                    src:LOC_GRPDIGITS is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:LOC_LASTSAVED), '1900-01-01')) is null and 
                    src:LOC_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LOC_MONFRACT), '0'), 38, 10) is null and 
                    src:LOC_MONFRACT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LOC_MONTGRPDIGITS), '0'), 38, 10) is null and 
                    src:LOC_MONTGRPDIGITS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LOC_STARTDAY), '0'), 38, 10) is null and 
                    src:LOC_STARTDAY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LOC_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:LOC_UPDATECOUNT is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:LOC_LASTSAVED), '1900-01-01')) is null and 
                src:LOC_LASTSAVED is not null)