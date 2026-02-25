CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5STANDWACTS_ERROR AS SELECT src, 'EAM_R5STANDWACTS' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WAC_ACT), '0'), 38, 10) is null and 
                    src:WAC_ACT is not null) THEN
                    'WAC_ACT contains a non-numeric value : \'' || AS_VARCHAR(src:WAC_ACT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WAC_CREATED), '1900-01-01')) is null and 
                    src:WAC_CREATED is not null) THEN
                    'WAC_CREATED contains a non-timestamp value : \'' || AS_VARCHAR(src:WAC_CREATED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WAC_DURATION), '0'), 38, 10) is null and 
                    src:WAC_DURATION is not null) THEN
                    'WAC_DURATION contains a non-numeric value : \'' || AS_VARCHAR(src:WAC_DURATION) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WAC_EST), '0'), 38, 10) is null and 
                    src:WAC_EST is not null) THEN
                    'WAC_EST contains a non-numeric value : \'' || AS_VARCHAR(src:WAC_EST) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WAC_LASTSAVED), '1900-01-01')) is null and 
                    src:WAC_LASTSAVED is not null) THEN
                    'WAC_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:WAC_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WAC_PERSONS), '0'), 38, 10) is null and 
                    src:WAC_PERSONS is not null) THEN
                    'WAC_PERSONS contains a non-numeric value : \'' || AS_VARCHAR(src:WAC_PERSONS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WAC_QTY), '0'), 38, 10) is null and 
                    src:WAC_QTY is not null) THEN
                    'WAC_QTY contains a non-numeric value : \'' || AS_VARCHAR(src:WAC_QTY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WAC_START), '0'), 38, 10) is null and 
                    src:WAC_START is not null) THEN
                    'WAC_START contains a non-numeric value : \'' || AS_VARCHAR(src:WAC_START) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WAC_UDFDATE01), '1900-01-01')) is null and 
                    src:WAC_UDFDATE01 is not null) THEN
                    'WAC_UDFDATE01 contains a non-timestamp value : \'' || AS_VARCHAR(src:WAC_UDFDATE01) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WAC_UDFDATE02), '1900-01-01')) is null and 
                    src:WAC_UDFDATE02 is not null) THEN
                    'WAC_UDFDATE02 contains a non-timestamp value : \'' || AS_VARCHAR(src:WAC_UDFDATE02) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WAC_UDFDATE03), '1900-01-01')) is null and 
                    src:WAC_UDFDATE03 is not null) THEN
                    'WAC_UDFDATE03 contains a non-timestamp value : \'' || AS_VARCHAR(src:WAC_UDFDATE03) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WAC_UDFDATE04), '1900-01-01')) is null and 
                    src:WAC_UDFDATE04 is not null) THEN
                    'WAC_UDFDATE04 contains a non-timestamp value : \'' || AS_VARCHAR(src:WAC_UDFDATE04) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WAC_UDFDATE05), '1900-01-01')) is null and 
                    src:WAC_UDFDATE05 is not null) THEN
                    'WAC_UDFDATE05 contains a non-timestamp value : \'' || AS_VARCHAR(src:WAC_UDFDATE05) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WAC_UDFNUM01), '0'), 38, 10) is null and 
                    src:WAC_UDFNUM01 is not null) THEN
                    'WAC_UDFNUM01 contains a non-numeric value : \'' || AS_VARCHAR(src:WAC_UDFNUM01) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WAC_UDFNUM02), '0'), 38, 10) is null and 
                    src:WAC_UDFNUM02 is not null) THEN
                    'WAC_UDFNUM02 contains a non-numeric value : \'' || AS_VARCHAR(src:WAC_UDFNUM02) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WAC_UDFNUM03), '0'), 38, 10) is null and 
                    src:WAC_UDFNUM03 is not null) THEN
                    'WAC_UDFNUM03 contains a non-numeric value : \'' || AS_VARCHAR(src:WAC_UDFNUM03) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WAC_UDFNUM04), '0'), 38, 10) is null and 
                    src:WAC_UDFNUM04 is not null) THEN
                    'WAC_UDFNUM04 contains a non-numeric value : \'' || AS_VARCHAR(src:WAC_UDFNUM04) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WAC_UDFNUM05), '0'), 38, 10) is null and 
                    src:WAC_UDFNUM05 is not null) THEN
                    'WAC_UDFNUM05 contains a non-numeric value : \'' || AS_VARCHAR(src:WAC_UDFNUM05) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WAC_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:WAC_UPDATECOUNT is not null) THEN
                    'WAC_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:WAC_UPDATECOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WAC_UPDATED), '1900-01-01')) is null and 
                    src:WAC_UPDATED is not null) THEN
                    'WAC_UPDATED contains a non-timestamp value : \'' || AS_VARCHAR(src:WAC_UPDATED) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WAC_LASTSAVED), '1900-01-01')) is null and 
                src:WAC_LASTSAVED is not null) THEN
                'WAC_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:WAC_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:WAC_ACT,
                src:WAC_STANDWORK  ORDER BY 
            src:WAC_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5STANDWACTS as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WAC_ACT), '0'), 38, 10) is null and 
                    src:WAC_ACT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WAC_CREATED), '1900-01-01')) is null and 
                    src:WAC_CREATED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WAC_DURATION), '0'), 38, 10) is null and 
                    src:WAC_DURATION is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WAC_EST), '0'), 38, 10) is null and 
                    src:WAC_EST is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WAC_LASTSAVED), '1900-01-01')) is null and 
                    src:WAC_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WAC_PERSONS), '0'), 38, 10) is null and 
                    src:WAC_PERSONS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WAC_QTY), '0'), 38, 10) is null and 
                    src:WAC_QTY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WAC_START), '0'), 38, 10) is null and 
                    src:WAC_START is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WAC_UDFDATE01), '1900-01-01')) is null and 
                    src:WAC_UDFDATE01 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WAC_UDFDATE02), '1900-01-01')) is null and 
                    src:WAC_UDFDATE02 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WAC_UDFDATE03), '1900-01-01')) is null and 
                    src:WAC_UDFDATE03 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WAC_UDFDATE04), '1900-01-01')) is null and 
                    src:WAC_UDFDATE04 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WAC_UDFDATE05), '1900-01-01')) is null and 
                    src:WAC_UDFDATE05 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WAC_UDFNUM01), '0'), 38, 10) is null and 
                    src:WAC_UDFNUM01 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WAC_UDFNUM02), '0'), 38, 10) is null and 
                    src:WAC_UDFNUM02 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WAC_UDFNUM03), '0'), 38, 10) is null and 
                    src:WAC_UDFNUM03 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WAC_UDFNUM04), '0'), 38, 10) is null and 
                    src:WAC_UDFNUM04 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WAC_UDFNUM05), '0'), 38, 10) is null and 
                    src:WAC_UDFNUM05 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WAC_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:WAC_UPDATECOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WAC_UPDATED), '1900-01-01')) is null and 
                    src:WAC_UPDATED is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WAC_LASTSAVED), '1900-01-01')) is null and 
                src:WAC_LASTSAVED is not null)