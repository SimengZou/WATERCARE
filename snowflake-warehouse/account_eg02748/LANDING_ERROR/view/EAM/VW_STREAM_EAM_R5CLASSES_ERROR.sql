CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5CLASSES_ERROR AS SELECT src, 'EAM_R5CLASSES' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CLS_CREATED), '1900-01-01')) is null and 
                    src:CLS_CREATED is not null) THEN
                    'CLS_CREATED contains a non-timestamp value : \'' || AS_VARCHAR(src:CLS_CREATED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CLS_LASTSAVED), '1900-01-01')) is null and 
                    src:CLS_LASTSAVED is not null) THEN
                    'CLS_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:CLS_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CLS_LEVEL), '0'), 38, 10) is null and 
                    src:CLS_LEVEL is not null) THEN
                    'CLS_LEVEL contains a non-numeric value : \'' || AS_VARCHAR(src:CLS_LEVEL) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CLS_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:CLS_UPDATECOUNT is not null) THEN
                    'CLS_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:CLS_UPDATECOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CLS_UPDATED), '1900-01-01')) is null and 
                    src:CLS_UPDATED is not null) THEN
                    'CLS_UPDATED contains a non-timestamp value : \'' || AS_VARCHAR(src:CLS_UPDATED) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CLS_LASTSAVED), '1900-01-01')) is null and 
                src:CLS_LASTSAVED is not null) THEN
                'CLS_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:CLS_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:CLS_CODE,
                src:CLS_ENTITY,
                src:CLS_ORG  ORDER BY 
            src:CLS_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5CLASSES as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CLS_CREATED), '1900-01-01')) is null and 
                    src:CLS_CREATED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CLS_LASTSAVED), '1900-01-01')) is null and 
                    src:CLS_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CLS_LEVEL), '0'), 38, 10) is null and 
                    src:CLS_LEVEL is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CLS_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:CLS_UPDATECOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CLS_UPDATED), '1900-01-01')) is null and 
                    src:CLS_UPDATED is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CLS_LASTSAVED), '1900-01-01')) is null and 
                src:CLS_LASTSAVED is not null)