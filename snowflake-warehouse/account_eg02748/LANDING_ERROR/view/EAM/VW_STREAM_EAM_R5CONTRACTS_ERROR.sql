CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5CONTRACTS_ERROR AS SELECT src, 'EAM_R5CONTRACTS' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CON_CREATE), '1900-01-01')) is null and 
                    src:CON_CREATE is not null) THEN
                    'CON_CREATE contains a non-timestamp value : \'' || AS_VARCHAR(src:CON_CREATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CON_END), '1900-01-01')) is null and 
                    src:CON_END is not null) THEN
                    'CON_END contains a non-timestamp value : \'' || AS_VARCHAR(src:CON_END) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CON_LASTSAVED), '1900-01-01')) is null and 
                    src:CON_LASTSAVED is not null) THEN
                    'CON_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:CON_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CON_RENEW), '1900-01-01')) is null and 
                    src:CON_RENEW is not null) THEN
                    'CON_RENEW contains a non-timestamp value : \'' || AS_VARCHAR(src:CON_RENEW) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CON_START), '1900-01-01')) is null and 
                    src:CON_START is not null) THEN
                    'CON_START contains a non-timestamp value : \'' || AS_VARCHAR(src:CON_START) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CON_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:CON_UPDATECOUNT is not null) THEN
                    'CON_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:CON_UPDATECOUNT) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CON_LASTSAVED), '1900-01-01')) is null and 
                src:CON_LASTSAVED is not null) THEN
                'CON_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:CON_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:CON_CODE  ORDER BY 
            src:CON_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5CONTRACTS as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CON_CREATE), '1900-01-01')) is null and 
                    src:CON_CREATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CON_END), '1900-01-01')) is null and 
                    src:CON_END is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CON_LASTSAVED), '1900-01-01')) is null and 
                    src:CON_LASTSAVED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CON_RENEW), '1900-01-01')) is null and 
                    src:CON_RENEW is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CON_START), '1900-01-01')) is null and 
                    src:CON_START is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CON_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:CON_UPDATECOUNT is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CON_LASTSAVED), '1900-01-01')) is null and 
                src:CON_LASTSAVED is not null)