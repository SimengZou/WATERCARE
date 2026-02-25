CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5PRECAUTIONS_ERROR AS SELECT src, 'EAM_R5PRECAUTIONS' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PCA_APPROVED), '1900-01-01')) is null and 
                    src:PCA_APPROVED is not null) THEN
                    'PCA_APPROVED contains a non-timestamp value : \'' || AS_VARCHAR(src:PCA_APPROVED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PCA_CREATED), '1900-01-01')) is null and 
                    src:PCA_CREATED is not null) THEN
                    'PCA_CREATED contains a non-timestamp value : \'' || AS_VARCHAR(src:PCA_CREATED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PCA_LASTSAVED), '1900-01-01')) is null and 
                    src:PCA_LASTSAVED is not null) THEN
                    'PCA_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:PCA_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PCA_REQUESTED), '1900-01-01')) is null and 
                    src:PCA_REQUESTED is not null) THEN
                    'PCA_REQUESTED contains a non-timestamp value : \'' || AS_VARCHAR(src:PCA_REQUESTED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PCA_REVIEWREQUIRED), '1900-01-01')) is null and 
                    src:PCA_REVIEWREQUIRED is not null) THEN
                    'PCA_REVIEWREQUIRED contains a non-timestamp value : \'' || AS_VARCHAR(src:PCA_REVIEWREQUIRED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PCA_REVISION), '0'), 38, 10) is null and 
                    src:PCA_REVISION is not null) THEN
                    'PCA_REVISION contains a non-numeric value : \'' || AS_VARCHAR(src:PCA_REVISION) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PCA_UDFDATE01), '1900-01-01')) is null and 
                    src:PCA_UDFDATE01 is not null) THEN
                    'PCA_UDFDATE01 contains a non-timestamp value : \'' || AS_VARCHAR(src:PCA_UDFDATE01) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PCA_UDFDATE02), '1900-01-01')) is null and 
                    src:PCA_UDFDATE02 is not null) THEN
                    'PCA_UDFDATE02 contains a non-timestamp value : \'' || AS_VARCHAR(src:PCA_UDFDATE02) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PCA_UDFDATE03), '1900-01-01')) is null and 
                    src:PCA_UDFDATE03 is not null) THEN
                    'PCA_UDFDATE03 contains a non-timestamp value : \'' || AS_VARCHAR(src:PCA_UDFDATE03) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PCA_UDFDATE04), '1900-01-01')) is null and 
                    src:PCA_UDFDATE04 is not null) THEN
                    'PCA_UDFDATE04 contains a non-timestamp value : \'' || AS_VARCHAR(src:PCA_UDFDATE04) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PCA_UDFDATE05), '1900-01-01')) is null and 
                    src:PCA_UDFDATE05 is not null) THEN
                    'PCA_UDFDATE05 contains a non-timestamp value : \'' || AS_VARCHAR(src:PCA_UDFDATE05) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PCA_UDFNUM01), '0'), 38, 10) is null and 
                    src:PCA_UDFNUM01 is not null) THEN
                    'PCA_UDFNUM01 contains a non-numeric value : \'' || AS_VARCHAR(src:PCA_UDFNUM01) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PCA_UDFNUM02), '0'), 38, 10) is null and 
                    src:PCA_UDFNUM02 is not null) THEN
                    'PCA_UDFNUM02 contains a non-numeric value : \'' || AS_VARCHAR(src:PCA_UDFNUM02) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PCA_UDFNUM03), '0'), 38, 10) is null and 
                    src:PCA_UDFNUM03 is not null) THEN
                    'PCA_UDFNUM03 contains a non-numeric value : \'' || AS_VARCHAR(src:PCA_UDFNUM03) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PCA_UDFNUM04), '0'), 38, 10) is null and 
                    src:PCA_UDFNUM04 is not null) THEN
                    'PCA_UDFNUM04 contains a non-numeric value : \'' || AS_VARCHAR(src:PCA_UDFNUM04) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PCA_UDFNUM05), '0'), 38, 10) is null and 
                    src:PCA_UDFNUM05 is not null) THEN
                    'PCA_UDFNUM05 contains a non-numeric value : \'' || AS_VARCHAR(src:PCA_UDFNUM05) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PCA_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:PCA_UPDATECOUNT is not null) THEN
                    'PCA_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:PCA_UPDATECOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PCA_UPDATED), '1900-01-01')) is null and 
                    src:PCA_UPDATED is not null) THEN
                    'PCA_UPDATED contains a non-timestamp value : \'' || AS_VARCHAR(src:PCA_UPDATED) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PCA_LASTSAVED), '1900-01-01')) is null and 
                src:PCA_LASTSAVED is not null) THEN
                'PCA_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:PCA_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:PCA_CODE,
                src:PCA_ORG,
                src:PCA_REVISION  ORDER BY 
            src:PCA_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5PRECAUTIONS as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PCA_APPROVED), '1900-01-01')) is null and 
                    src:PCA_APPROVED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PCA_CREATED), '1900-01-01')) is null and 
                    src:PCA_CREATED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PCA_LASTSAVED), '1900-01-01')) is null and 
                    src:PCA_LASTSAVED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PCA_REQUESTED), '1900-01-01')) is null and 
                    src:PCA_REQUESTED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PCA_REVIEWREQUIRED), '1900-01-01')) is null and 
                    src:PCA_REVIEWREQUIRED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PCA_REVISION), '0'), 38, 10) is null and 
                    src:PCA_REVISION is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PCA_UDFDATE01), '1900-01-01')) is null and 
                    src:PCA_UDFDATE01 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PCA_UDFDATE02), '1900-01-01')) is null and 
                    src:PCA_UDFDATE02 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PCA_UDFDATE03), '1900-01-01')) is null and 
                    src:PCA_UDFDATE03 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PCA_UDFDATE04), '1900-01-01')) is null and 
                    src:PCA_UDFDATE04 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PCA_UDFDATE05), '1900-01-01')) is null and 
                    src:PCA_UDFDATE05 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PCA_UDFNUM01), '0'), 38, 10) is null and 
                    src:PCA_UDFNUM01 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PCA_UDFNUM02), '0'), 38, 10) is null and 
                    src:PCA_UDFNUM02 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PCA_UDFNUM03), '0'), 38, 10) is null and 
                    src:PCA_UDFNUM03 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PCA_UDFNUM04), '0'), 38, 10) is null and 
                    src:PCA_UDFNUM04 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PCA_UDFNUM05), '0'), 38, 10) is null and 
                    src:PCA_UDFNUM05 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PCA_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:PCA_UPDATECOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PCA_UPDATED), '1900-01-01')) is null and 
                    src:PCA_UPDATED is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PCA_LASTSAVED), '1900-01-01')) is null and 
                src:PCA_LASTSAVED is not null)