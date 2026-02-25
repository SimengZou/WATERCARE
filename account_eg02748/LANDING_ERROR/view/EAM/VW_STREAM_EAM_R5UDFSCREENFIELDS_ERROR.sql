CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5UDFSCREENFIELDS_ERROR AS SELECT src, 'EAM_R5UDFSCREENFIELDS' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:USF_CREATED), '1900-01-01')) is null and 
                    src:USF_CREATED is not null) THEN
                    'USF_CREATED contains a non-timestamp value : \'' || AS_VARCHAR(src:USF_CREATED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:USF_LASTSAVED), '1900-01-01')) is null and 
                    src:USF_LASTSAVED is not null) THEN
                    'USF_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:USF_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USF_MAXLENGTH), '0'), 38, 10) is null and 
                    src:USF_MAXLENGTH is not null) THEN
                    'USF_MAXLENGTH contains a non-numeric value : \'' || AS_VARCHAR(src:USF_MAXLENGTH) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USF_PRECISION), '0'), 38, 10) is null and 
                    src:USF_PRECISION is not null) THEN
                    'USF_PRECISION contains a non-numeric value : \'' || AS_VARCHAR(src:USF_PRECISION) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USF_SCALE), '0'), 38, 10) is null and 
                    src:USF_SCALE is not null) THEN
                    'USF_SCALE contains a non-numeric value : \'' || AS_VARCHAR(src:USF_SCALE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USF_SEQUENCE), '0'), 38, 10) is null and 
                    src:USF_SEQUENCE is not null) THEN
                    'USF_SEQUENCE contains a non-numeric value : \'' || AS_VARCHAR(src:USF_SEQUENCE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USF_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:USF_UPDATECOUNT is not null) THEN
                    'USF_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:USF_UPDATECOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:USF_UPDATED), '1900-01-01')) is null and 
                    src:USF_UPDATED is not null) THEN
                    'USF_UPDATED contains a non-timestamp value : \'' || AS_VARCHAR(src:USF_UPDATED) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:USF_LASTSAVED), '1900-01-01')) is null and 
                src:USF_LASTSAVED is not null) THEN
                'USF_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:USF_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:USF_FIELDNAME,
                src:USF_SCREENNAME  ORDER BY 
            src:USF_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5UDFSCREENFIELDS as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:USF_CREATED), '1900-01-01')) is null and 
                    src:USF_CREATED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:USF_LASTSAVED), '1900-01-01')) is null and 
                    src:USF_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USF_MAXLENGTH), '0'), 38, 10) is null and 
                    src:USF_MAXLENGTH is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USF_PRECISION), '0'), 38, 10) is null and 
                    src:USF_PRECISION is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USF_SCALE), '0'), 38, 10) is null and 
                    src:USF_SCALE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USF_SEQUENCE), '0'), 38, 10) is null and 
                    src:USF_SEQUENCE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USF_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:USF_UPDATECOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:USF_UPDATED), '1900-01-01')) is null and 
                    src:USF_UPDATED is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:USF_LASTSAVED), '1900-01-01')) is null and 
                src:USF_LASTSAVED is not null)