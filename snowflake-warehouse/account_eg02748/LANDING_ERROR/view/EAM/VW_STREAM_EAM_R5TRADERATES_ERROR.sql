CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5TRADERATES_ERROR AS SELECT src, 'EAM_R5TRADERATES' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRR_END), '1900-01-01')) is null and 
                    src:TRR_END is not null) THEN
                    'TRR_END contains a non-timestamp value : \'' || AS_VARCHAR(src:TRR_END) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRR_LASTSAVED), '1900-01-01')) is null and 
                    src:TRR_LASTSAVED is not null) THEN
                    'TRR_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:TRR_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRR_NTRATE), '0'), 38, 10) is null and 
                    src:TRR_NTRATE is not null) THEN
                    'TRR_NTRATE contains a non-numeric value : \'' || AS_VARCHAR(src:TRR_NTRATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRR_OTRATE), '0'), 38, 10) is null and 
                    src:TRR_OTRATE is not null) THEN
                    'TRR_OTRATE contains a non-numeric value : \'' || AS_VARCHAR(src:TRR_OTRATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRR_START), '1900-01-01')) is null and 
                    src:TRR_START is not null) THEN
                    'TRR_START contains a non-timestamp value : \'' || AS_VARCHAR(src:TRR_START) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRR_UDFDATE01), '1900-01-01')) is null and 
                    src:TRR_UDFDATE01 is not null) THEN
                    'TRR_UDFDATE01 contains a non-timestamp value : \'' || AS_VARCHAR(src:TRR_UDFDATE01) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRR_UDFDATE02), '1900-01-01')) is null and 
                    src:TRR_UDFDATE02 is not null) THEN
                    'TRR_UDFDATE02 contains a non-timestamp value : \'' || AS_VARCHAR(src:TRR_UDFDATE02) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRR_UDFDATE03), '1900-01-01')) is null and 
                    src:TRR_UDFDATE03 is not null) THEN
                    'TRR_UDFDATE03 contains a non-timestamp value : \'' || AS_VARCHAR(src:TRR_UDFDATE03) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRR_UDFDATE04), '1900-01-01')) is null and 
                    src:TRR_UDFDATE04 is not null) THEN
                    'TRR_UDFDATE04 contains a non-timestamp value : \'' || AS_VARCHAR(src:TRR_UDFDATE04) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRR_UDFDATE05), '1900-01-01')) is null and 
                    src:TRR_UDFDATE05 is not null) THEN
                    'TRR_UDFDATE05 contains a non-timestamp value : \'' || AS_VARCHAR(src:TRR_UDFDATE05) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRR_UDFNUM01), '0'), 38, 10) is null and 
                    src:TRR_UDFNUM01 is not null) THEN
                    'TRR_UDFNUM01 contains a non-numeric value : \'' || AS_VARCHAR(src:TRR_UDFNUM01) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRR_UDFNUM02), '0'), 38, 10) is null and 
                    src:TRR_UDFNUM02 is not null) THEN
                    'TRR_UDFNUM02 contains a non-numeric value : \'' || AS_VARCHAR(src:TRR_UDFNUM02) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRR_UDFNUM03), '0'), 38, 10) is null and 
                    src:TRR_UDFNUM03 is not null) THEN
                    'TRR_UDFNUM03 contains a non-numeric value : \'' || AS_VARCHAR(src:TRR_UDFNUM03) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRR_UDFNUM04), '0'), 38, 10) is null and 
                    src:TRR_UDFNUM04 is not null) THEN
                    'TRR_UDFNUM04 contains a non-numeric value : \'' || AS_VARCHAR(src:TRR_UDFNUM04) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRR_UDFNUM05), '0'), 38, 10) is null and 
                    src:TRR_UDFNUM05 is not null) THEN
                    'TRR_UDFNUM05 contains a non-numeric value : \'' || AS_VARCHAR(src:TRR_UDFNUM05) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRR_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:TRR_UPDATECOUNT is not null) THEN
                    'TRR_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:TRR_UPDATECOUNT) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRR_LASTSAVED), '1900-01-01')) is null and 
                src:TRR_LASTSAVED is not null) THEN
                'TRR_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:TRR_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:TRR_MRC,
                src:TRR_OCTYPE,
                src:TRR_ORG,
                src:TRR_PERSON,
                src:TRR_START,
                src:TRR_SUPPLIER,
                src:TRR_SUPPLIER_ORG,
                src:TRR_TRADE  ORDER BY 
            src:TRR_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5TRADERATES as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRR_END), '1900-01-01')) is null and 
                    src:TRR_END is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRR_LASTSAVED), '1900-01-01')) is null and 
                    src:TRR_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRR_NTRATE), '0'), 38, 10) is null and 
                    src:TRR_NTRATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRR_OTRATE), '0'), 38, 10) is null and 
                    src:TRR_OTRATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRR_START), '1900-01-01')) is null and 
                    src:TRR_START is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRR_UDFDATE01), '1900-01-01')) is null and 
                    src:TRR_UDFDATE01 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRR_UDFDATE02), '1900-01-01')) is null and 
                    src:TRR_UDFDATE02 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRR_UDFDATE03), '1900-01-01')) is null and 
                    src:TRR_UDFDATE03 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRR_UDFDATE04), '1900-01-01')) is null and 
                    src:TRR_UDFDATE04 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRR_UDFDATE05), '1900-01-01')) is null and 
                    src:TRR_UDFDATE05 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRR_UDFNUM01), '0'), 38, 10) is null and 
                    src:TRR_UDFNUM01 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRR_UDFNUM02), '0'), 38, 10) is null and 
                    src:TRR_UDFNUM02 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRR_UDFNUM03), '0'), 38, 10) is null and 
                    src:TRR_UDFNUM03 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRR_UDFNUM04), '0'), 38, 10) is null and 
                    src:TRR_UDFNUM04 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRR_UDFNUM05), '0'), 38, 10) is null and 
                    src:TRR_UDFNUM05 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRR_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:TRR_UPDATECOUNT is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRR_LASTSAVED), '1900-01-01')) is null and 
                src:TRR_LASTSAVED is not null)