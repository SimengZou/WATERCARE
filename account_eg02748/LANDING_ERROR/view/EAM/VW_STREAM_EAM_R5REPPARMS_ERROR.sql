CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5REPPARMS_ERROR AS SELECT src, 'EAM_R5REPPARMS' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PMT_LASTSAVED), '1900-01-01')) is null and 
                    src:PMT_LASTSAVED is not null) THEN
                    'PMT_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:PMT_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PMT_LENGTH), '0'), 38, 10) is null and 
                    src:PMT_LENGTH is not null) THEN
                    'PMT_LENGTH contains a non-numeric value : \'' || AS_VARCHAR(src:PMT_LENGTH) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PMT_LINE), '0'), 38, 10) is null and 
                    src:PMT_LINE is not null) THEN
                    'PMT_LINE contains a non-numeric value : \'' || AS_VARCHAR(src:PMT_LINE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PMT_OPTIONS), '0'), 38, 10) is null and 
                    src:PMT_OPTIONS is not null) THEN
                    'PMT_OPTIONS contains a non-numeric value : \'' || AS_VARCHAR(src:PMT_OPTIONS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PMT_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:PMT_UPDATECOUNT is not null) THEN
                    'PMT_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:PMT_UPDATECOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PMT_UPDATED), '1900-01-01')) is null and 
                    src:PMT_UPDATED is not null) THEN
                    'PMT_UPDATED contains a non-timestamp value : \'' || AS_VARCHAR(src:PMT_UPDATED) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PMT_LASTSAVED), '1900-01-01')) is null and 
                src:PMT_LASTSAVED is not null) THEN
                'PMT_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:PMT_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:PMT_FUNCTION,
                src:PMT_PARAMETER  ORDER BY 
            src:PMT_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5REPPARMS as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PMT_LASTSAVED), '1900-01-01')) is null and 
                    src:PMT_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PMT_LENGTH), '0'), 38, 10) is null and 
                    src:PMT_LENGTH is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PMT_LINE), '0'), 38, 10) is null and 
                    src:PMT_LINE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PMT_OPTIONS), '0'), 38, 10) is null and 
                    src:PMT_OPTIONS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PMT_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:PMT_UPDATECOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PMT_UPDATED), '1900-01-01')) is null and 
                    src:PMT_UPDATED is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PMT_LASTSAVED), '1900-01-01')) is null and 
                src:PMT_LASTSAVED is not null)