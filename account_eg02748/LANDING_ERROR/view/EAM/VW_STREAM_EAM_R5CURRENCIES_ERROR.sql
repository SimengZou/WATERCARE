CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5CURRENCIES_ERROR AS SELECT src, 'EAM_R5CURRENCIES' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CUR_CREATED), '1900-01-01')) is null and 
                    src:CUR_CREATED is not null) THEN
                    'CUR_CREATED contains a non-timestamp value : \'' || AS_VARCHAR(src:CUR_CREATED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CUR_DUAL), '0'), 38, 10) is null and 
                    src:CUR_DUAL is not null) THEN
                    'CUR_DUAL contains a non-numeric value : \'' || AS_VARCHAR(src:CUR_DUAL) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CUR_INTERFACE), '1900-01-01')) is null and 
                    src:CUR_INTERFACE is not null) THEN
                    'CUR_INTERFACE contains a non-timestamp value : \'' || AS_VARCHAR(src:CUR_INTERFACE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CUR_LASTSAVED), '1900-01-01')) is null and 
                    src:CUR_LASTSAVED is not null) THEN
                    'CUR_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:CUR_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CUR_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:CUR_UPDATECOUNT is not null) THEN
                    'CUR_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:CUR_UPDATECOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CUR_UPDATED), '1900-01-01')) is null and 
                    src:CUR_UPDATED is not null) THEN
                    'CUR_UPDATED contains a non-timestamp value : \'' || AS_VARCHAR(src:CUR_UPDATED) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CUR_LASTSAVED), '1900-01-01')) is null and 
                src:CUR_LASTSAVED is not null) THEN
                'CUR_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:CUR_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:CUR_CODE  ORDER BY 
            src:CUR_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5CURRENCIES as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CUR_CREATED), '1900-01-01')) is null and 
                    src:CUR_CREATED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CUR_DUAL), '0'), 38, 10) is null and 
                    src:CUR_DUAL is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CUR_INTERFACE), '1900-01-01')) is null and 
                    src:CUR_INTERFACE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CUR_LASTSAVED), '1900-01-01')) is null and 
                    src:CUR_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CUR_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:CUR_UPDATECOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CUR_UPDATED), '1900-01-01')) is null and 
                    src:CUR_UPDATED is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CUR_LASTSAVED), '1900-01-01')) is null and 
                src:CUR_LASTSAVED is not null)