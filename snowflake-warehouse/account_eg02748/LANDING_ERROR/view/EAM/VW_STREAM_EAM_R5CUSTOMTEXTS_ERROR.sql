CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5CUSTOMTEXTS_ERROR AS SELECT src, 'EAM_R5CUSTOMTEXTS' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CTT_DATE), '1900-01-01')) is null and 
                    src:CTT_DATE is not null) THEN
                    'CTT_DATE contains a non-timestamp value : \'' || AS_VARCHAR(src:CTT_DATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CTT_LASTSAVED), '1900-01-01')) is null and 
                    src:CTT_LASTSAVED is not null) THEN
                    'CTT_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:CTT_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CTT_LENGTH), '0'), 38, 10) is null and 
                    src:CTT_LENGTH is not null) THEN
                    'CTT_LENGTH contains a non-numeric value : \'' || AS_VARCHAR(src:CTT_LENGTH) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CTT_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:CTT_UPDATECOUNT is not null) THEN
                    'CTT_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:CTT_UPDATECOUNT) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CTT_LASTSAVED), '1900-01-01')) is null and 
                src:CTT_LASTSAVED is not null) THEN
                'CTT_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:CTT_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:CTT_LANG,
                src:CTT_LENGTH,
                src:CTT_POOL  ORDER BY 
            src:CTT_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5CUSTOMTEXTS as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CTT_DATE), '1900-01-01')) is null and 
                    src:CTT_DATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CTT_LASTSAVED), '1900-01-01')) is null and 
                    src:CTT_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CTT_LENGTH), '0'), 38, 10) is null and 
                    src:CTT_LENGTH is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CTT_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:CTT_UPDATECOUNT is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CTT_LASTSAVED), '1900-01-01')) is null and 
                src:CTT_LASTSAVED is not null)