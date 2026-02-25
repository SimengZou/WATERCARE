CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5UCODES_ERROR AS SELECT src, 'EAM_R5UCODES' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:UCO_CREATED), '1900-01-01')) is null and 
                    src:UCO_CREATED is not null) THEN
                    'UCO_CREATED contains a non-timestamp value : \'' || AS_VARCHAR(src:UCO_CREATED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:UCO_GISDISPATCHRANKING), '0'), 38, 10) is null and 
                    src:UCO_GISDISPATCHRANKING is not null) THEN
                    'UCO_GISDISPATCHRANKING contains a non-numeric value : \'' || AS_VARCHAR(src:UCO_GISDISPATCHRANKING) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:UCO_LASTSAVED), '1900-01-01')) is null and 
                    src:UCO_LASTSAVED is not null) THEN
                    'UCO_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:UCO_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:UCO_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:UCO_UPDATECOUNT is not null) THEN
                    'UCO_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:UCO_UPDATECOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:UCO_UPDATED), '1900-01-01')) is null and 
                    src:UCO_UPDATED is not null) THEN
                    'UCO_UPDATED contains a non-timestamp value : \'' || AS_VARCHAR(src:UCO_UPDATED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:UCO_WEIGHT), '0'), 38, 10) is null and 
                    src:UCO_WEIGHT is not null) THEN
                    'UCO_WEIGHT contains a non-numeric value : \'' || AS_VARCHAR(src:UCO_WEIGHT) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:UCO_LASTSAVED), '1900-01-01')) is null and 
                src:UCO_LASTSAVED is not null) THEN
                'UCO_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:UCO_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:UCO_CODE,
                src:UCO_ENTITY  ORDER BY 
            src:UCO_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5UCODES as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:UCO_CREATED), '1900-01-01')) is null and 
                    src:UCO_CREATED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:UCO_GISDISPATCHRANKING), '0'), 38, 10) is null and 
                    src:UCO_GISDISPATCHRANKING is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:UCO_LASTSAVED), '1900-01-01')) is null and 
                    src:UCO_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:UCO_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:UCO_UPDATECOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:UCO_UPDATED), '1900-01-01')) is null and 
                    src:UCO_UPDATED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:UCO_WEIGHT), '0'), 38, 10) is null and 
                    src:UCO_WEIGHT is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:UCO_LASTSAVED), '1900-01-01')) is null and 
                src:UCO_LASTSAVED is not null)