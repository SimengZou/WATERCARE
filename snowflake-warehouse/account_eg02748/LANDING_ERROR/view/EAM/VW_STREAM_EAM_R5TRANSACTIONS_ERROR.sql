CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5TRANSACTIONS_ERROR AS SELECT src, 'EAM_R5TRANSACTIONS' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRA_ACD), '0'), 38, 10) is null and 
                    src:TRA_ACD is not null) THEN
                    'TRA_ACD contains a non-numeric value : \'' || AS_VARCHAR(src:TRA_ACD) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRA_CREATED), '1900-01-01')) is null and 
                    src:TRA_CREATED is not null) THEN
                    'TRA_CREATED contains a non-timestamp value : \'' || AS_VARCHAR(src:TRA_CREATED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRA_DATE), '1900-01-01')) is null and 
                    src:TRA_DATE is not null) THEN
                    'TRA_DATE contains a non-timestamp value : \'' || AS_VARCHAR(src:TRA_DATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRA_DCKLINE), '0'), 38, 10) is null and 
                    src:TRA_DCKLINE is not null) THEN
                    'TRA_DCKLINE contains a non-numeric value : \'' || AS_VARCHAR(src:TRA_DCKLINE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRA_INTERFACE), '1900-01-01')) is null and 
                    src:TRA_INTERFACE is not null) THEN
                    'TRA_INTERFACE contains a non-timestamp value : \'' || AS_VARCHAR(src:TRA_INTERFACE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRA_INVALLOCATION), '0'), 38, 10) is null and 
                    src:TRA_INVALLOCATION is not null) THEN
                    'TRA_INVALLOCATION contains a non-numeric value : \'' || AS_VARCHAR(src:TRA_INVALLOCATION) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRA_LASTSAVED), '1900-01-01')) is null and 
                    src:TRA_LASTSAVED is not null) THEN
                    'TRA_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:TRA_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRA_UDFDATE01), '1900-01-01')) is null and 
                    src:TRA_UDFDATE01 is not null) THEN
                    'TRA_UDFDATE01 contains a non-timestamp value : \'' || AS_VARCHAR(src:TRA_UDFDATE01) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRA_UDFDATE02), '1900-01-01')) is null and 
                    src:TRA_UDFDATE02 is not null) THEN
                    'TRA_UDFDATE02 contains a non-timestamp value : \'' || AS_VARCHAR(src:TRA_UDFDATE02) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRA_UDFDATE03), '1900-01-01')) is null and 
                    src:TRA_UDFDATE03 is not null) THEN
                    'TRA_UDFDATE03 contains a non-timestamp value : \'' || AS_VARCHAR(src:TRA_UDFDATE03) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRA_UDFDATE04), '1900-01-01')) is null and 
                    src:TRA_UDFDATE04 is not null) THEN
                    'TRA_UDFDATE04 contains a non-timestamp value : \'' || AS_VARCHAR(src:TRA_UDFDATE04) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRA_UDFDATE05), '1900-01-01')) is null and 
                    src:TRA_UDFDATE05 is not null) THEN
                    'TRA_UDFDATE05 contains a non-timestamp value : \'' || AS_VARCHAR(src:TRA_UDFDATE05) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRA_UDFNUM01), '0'), 38, 10) is null and 
                    src:TRA_UDFNUM01 is not null) THEN
                    'TRA_UDFNUM01 contains a non-numeric value : \'' || AS_VARCHAR(src:TRA_UDFNUM01) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRA_UDFNUM02), '0'), 38, 10) is null and 
                    src:TRA_UDFNUM02 is not null) THEN
                    'TRA_UDFNUM02 contains a non-numeric value : \'' || AS_VARCHAR(src:TRA_UDFNUM02) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRA_UDFNUM03), '0'), 38, 10) is null and 
                    src:TRA_UDFNUM03 is not null) THEN
                    'TRA_UDFNUM03 contains a non-numeric value : \'' || AS_VARCHAR(src:TRA_UDFNUM03) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRA_UDFNUM04), '0'), 38, 10) is null and 
                    src:TRA_UDFNUM04 is not null) THEN
                    'TRA_UDFNUM04 contains a non-numeric value : \'' || AS_VARCHAR(src:TRA_UDFNUM04) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRA_UDFNUM05), '0'), 38, 10) is null and 
                    src:TRA_UDFNUM05 is not null) THEN
                    'TRA_UDFNUM05 contains a non-numeric value : \'' || AS_VARCHAR(src:TRA_UDFNUM05) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRA_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:TRA_UPDATECOUNT is not null) THEN
                    'TRA_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:TRA_UPDATECOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRA_UPDATED), '1900-01-01')) is null and 
                    src:TRA_UPDATED is not null) THEN
                    'TRA_UPDATED contains a non-timestamp value : \'' || AS_VARCHAR(src:TRA_UPDATED) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRA_LASTSAVED), '1900-01-01')) is null and 
                src:TRA_LASTSAVED is not null) THEN
                'TRA_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:TRA_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:TRA_CODE  ORDER BY 
            src:TRA_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5TRANSACTIONS as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRA_ACD), '0'), 38, 10) is null and 
                    src:TRA_ACD is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRA_CREATED), '1900-01-01')) is null and 
                    src:TRA_CREATED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRA_DATE), '1900-01-01')) is null and 
                    src:TRA_DATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRA_DCKLINE), '0'), 38, 10) is null and 
                    src:TRA_DCKLINE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRA_INTERFACE), '1900-01-01')) is null and 
                    src:TRA_INTERFACE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRA_INVALLOCATION), '0'), 38, 10) is null and 
                    src:TRA_INVALLOCATION is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRA_LASTSAVED), '1900-01-01')) is null and 
                    src:TRA_LASTSAVED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRA_UDFDATE01), '1900-01-01')) is null and 
                    src:TRA_UDFDATE01 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRA_UDFDATE02), '1900-01-01')) is null and 
                    src:TRA_UDFDATE02 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRA_UDFDATE03), '1900-01-01')) is null and 
                    src:TRA_UDFDATE03 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRA_UDFDATE04), '1900-01-01')) is null and 
                    src:TRA_UDFDATE04 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRA_UDFDATE05), '1900-01-01')) is null and 
                    src:TRA_UDFDATE05 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRA_UDFNUM01), '0'), 38, 10) is null and 
                    src:TRA_UDFNUM01 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRA_UDFNUM02), '0'), 38, 10) is null and 
                    src:TRA_UDFNUM02 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRA_UDFNUM03), '0'), 38, 10) is null and 
                    src:TRA_UDFNUM03 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRA_UDFNUM04), '0'), 38, 10) is null and 
                    src:TRA_UDFNUM04 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRA_UDFNUM05), '0'), 38, 10) is null and 
                    src:TRA_UDFNUM05 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRA_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:TRA_UPDATECOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRA_UPDATED), '1900-01-01')) is null and 
                    src:TRA_UPDATED is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRA_LASTSAVED), '1900-01-01')) is null and 
                src:TRA_LASTSAVED is not null)