CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5PPMACTS_ERROR AS SELECT src, 'EAM_R5PPMACTS' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPA_ACT), '0'), 38, 10) is null and 
                    src:PPA_ACT is not null) THEN
                    'PPA_ACT contains a non-numeric value : \'' || AS_VARCHAR(src:PPA_ACT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPA_DURATION), '0'), 38, 10) is null and 
                    src:PPA_DURATION is not null) THEN
                    'PPA_DURATION contains a non-numeric value : \'' || AS_VARCHAR(src:PPA_DURATION) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPA_EST), '0'), 38, 10) is null and 
                    src:PPA_EST is not null) THEN
                    'PPA_EST contains a non-numeric value : \'' || AS_VARCHAR(src:PPA_EST) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPA_LASTSAVED), '1900-01-01')) is null and 
                    src:PPA_LASTSAVED is not null) THEN
                    'PPA_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:PPA_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPA_PERSONS), '0'), 38, 10) is null and 
                    src:PPA_PERSONS is not null) THEN
                    'PPA_PERSONS contains a non-numeric value : \'' || AS_VARCHAR(src:PPA_PERSONS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPA_QTY), '0'), 38, 10) is null and 
                    src:PPA_QTY is not null) THEN
                    'PPA_QTY contains a non-numeric value : \'' || AS_VARCHAR(src:PPA_QTY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPA_REVISION), '0'), 38, 10) is null and 
                    src:PPA_REVISION is not null) THEN
                    'PPA_REVISION contains a non-numeric value : \'' || AS_VARCHAR(src:PPA_REVISION) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPA_START), '0'), 38, 10) is null and 
                    src:PPA_START is not null) THEN
                    'PPA_START contains a non-numeric value : \'' || AS_VARCHAR(src:PPA_START) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPA_UDFDATE01), '1900-01-01')) is null and 
                    src:PPA_UDFDATE01 is not null) THEN
                    'PPA_UDFDATE01 contains a non-timestamp value : \'' || AS_VARCHAR(src:PPA_UDFDATE01) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPA_UDFDATE02), '1900-01-01')) is null and 
                    src:PPA_UDFDATE02 is not null) THEN
                    'PPA_UDFDATE02 contains a non-timestamp value : \'' || AS_VARCHAR(src:PPA_UDFDATE02) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPA_UDFDATE03), '1900-01-01')) is null and 
                    src:PPA_UDFDATE03 is not null) THEN
                    'PPA_UDFDATE03 contains a non-timestamp value : \'' || AS_VARCHAR(src:PPA_UDFDATE03) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPA_UDFDATE04), '1900-01-01')) is null and 
                    src:PPA_UDFDATE04 is not null) THEN
                    'PPA_UDFDATE04 contains a non-timestamp value : \'' || AS_VARCHAR(src:PPA_UDFDATE04) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPA_UDFDATE05), '1900-01-01')) is null and 
                    src:PPA_UDFDATE05 is not null) THEN
                    'PPA_UDFDATE05 contains a non-timestamp value : \'' || AS_VARCHAR(src:PPA_UDFDATE05) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPA_UDFNUM01), '0'), 38, 10) is null and 
                    src:PPA_UDFNUM01 is not null) THEN
                    'PPA_UDFNUM01 contains a non-numeric value : \'' || AS_VARCHAR(src:PPA_UDFNUM01) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPA_UDFNUM02), '0'), 38, 10) is null and 
                    src:PPA_UDFNUM02 is not null) THEN
                    'PPA_UDFNUM02 contains a non-numeric value : \'' || AS_VARCHAR(src:PPA_UDFNUM02) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPA_UDFNUM03), '0'), 38, 10) is null and 
                    src:PPA_UDFNUM03 is not null) THEN
                    'PPA_UDFNUM03 contains a non-numeric value : \'' || AS_VARCHAR(src:PPA_UDFNUM03) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPA_UDFNUM04), '0'), 38, 10) is null and 
                    src:PPA_UDFNUM04 is not null) THEN
                    'PPA_UDFNUM04 contains a non-numeric value : \'' || AS_VARCHAR(src:PPA_UDFNUM04) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPA_UDFNUM05), '0'), 38, 10) is null and 
                    src:PPA_UDFNUM05 is not null) THEN
                    'PPA_UDFNUM05 contains a non-numeric value : \'' || AS_VARCHAR(src:PPA_UDFNUM05) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPA_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:PPA_UPDATECOUNT is not null) THEN
                    'PPA_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:PPA_UPDATECOUNT) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPA_LASTSAVED), '1900-01-01')) is null and 
                src:PPA_LASTSAVED is not null) THEN
                'PPA_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:PPA_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:PPA_ACT,
                src:PPA_PPM,
                src:PPA_REVISION  ORDER BY 
            src:PPA_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5PPMACTS as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPA_ACT), '0'), 38, 10) is null and 
                    src:PPA_ACT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPA_DURATION), '0'), 38, 10) is null and 
                    src:PPA_DURATION is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPA_EST), '0'), 38, 10) is null and 
                    src:PPA_EST is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPA_LASTSAVED), '1900-01-01')) is null and 
                    src:PPA_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPA_PERSONS), '0'), 38, 10) is null and 
                    src:PPA_PERSONS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPA_QTY), '0'), 38, 10) is null and 
                    src:PPA_QTY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPA_REVISION), '0'), 38, 10) is null and 
                    src:PPA_REVISION is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPA_START), '0'), 38, 10) is null and 
                    src:PPA_START is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPA_UDFDATE01), '1900-01-01')) is null and 
                    src:PPA_UDFDATE01 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPA_UDFDATE02), '1900-01-01')) is null and 
                    src:PPA_UDFDATE02 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPA_UDFDATE03), '1900-01-01')) is null and 
                    src:PPA_UDFDATE03 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPA_UDFDATE04), '1900-01-01')) is null and 
                    src:PPA_UDFDATE04 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPA_UDFDATE05), '1900-01-01')) is null and 
                    src:PPA_UDFDATE05 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPA_UDFNUM01), '0'), 38, 10) is null and 
                    src:PPA_UDFNUM01 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPA_UDFNUM02), '0'), 38, 10) is null and 
                    src:PPA_UDFNUM02 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPA_UDFNUM03), '0'), 38, 10) is null and 
                    src:PPA_UDFNUM03 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPA_UDFNUM04), '0'), 38, 10) is null and 
                    src:PPA_UDFNUM04 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPA_UDFNUM05), '0'), 38, 10) is null and 
                    src:PPA_UDFNUM05 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPA_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:PPA_UPDATECOUNT is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPA_LASTSAVED), '1900-01-01')) is null and 
                src:PPA_LASTSAVED is not null)