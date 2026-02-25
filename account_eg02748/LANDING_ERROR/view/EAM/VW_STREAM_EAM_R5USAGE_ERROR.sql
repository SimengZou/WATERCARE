CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5USAGE_ERROR AS SELECT src, 'EAM_R5USAGE' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SUS_ACTION_DATE), '1900-01-01')) is null and 
                    src:SUS_ACTION_DATE is not null) THEN
                    'SUS_ACTION_DATE contains a non-timestamp value : \'' || AS_VARCHAR(src:SUS_ACTION_DATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SUS_LASTSAVED), '1900-01-01')) is null and 
                    src:SUS_LASTSAVED is not null) THEN
                    'SUS_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:SUS_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SUS_PROCESSING_TIME), '0'), 38, 10) is null and 
                    src:SUS_PROCESSING_TIME is not null) THEN
                    'SUS_PROCESSING_TIME contains a non-numeric value : \'' || AS_VARCHAR(src:SUS_PROCESSING_TIME) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SUS_SESSIONID), '0'), 38, 10) is null and 
                    src:SUS_SESSIONID is not null) THEN
                    'SUS_SESSIONID contains a non-numeric value : \'' || AS_VARCHAR(src:SUS_SESSIONID) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SUS_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:SUS_UPDATECOUNT is not null) THEN
                    'SUS_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:SUS_UPDATECOUNT) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SUS_LASTSAVED), '1900-01-01')) is null and 
                src:SUS_LASTSAVED is not null) THEN
                'SUS_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:SUS_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:SUS_ID  ORDER BY 
            src:SUS_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5USAGE as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SUS_ACTION_DATE), '1900-01-01')) is null and 
                    src:SUS_ACTION_DATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SUS_LASTSAVED), '1900-01-01')) is null and 
                    src:SUS_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SUS_PROCESSING_TIME), '0'), 38, 10) is null and 
                    src:SUS_PROCESSING_TIME is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SUS_SESSIONID), '0'), 38, 10) is null and 
                    src:SUS_SESSIONID is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SUS_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:SUS_UPDATECOUNT is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SUS_LASTSAVED), '1900-01-01')) is null and 
                src:SUS_LASTSAVED is not null)