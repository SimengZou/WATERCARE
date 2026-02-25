CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5ORDERS_ERROR AS SELECT src, 'EAM_R5ORDERS' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORD_ACD), '0'), 38, 10) is null and 
                    src:ORD_ACD is not null) THEN
                    'ORD_ACD contains a non-numeric value : \'' || AS_VARCHAR(src:ORD_ACD) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORD_APPROV), '1900-01-01')) is null and 
                    src:ORD_APPROV is not null) THEN
                    'ORD_APPROV contains a non-timestamp value : \'' || AS_VARCHAR(src:ORD_APPROV) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORD_BLANKETRELEASE), '0'), 38, 10) is null and 
                    src:ORD_BLANKETRELEASE is not null) THEN
                    'ORD_BLANKETRELEASE contains a non-numeric value : \'' || AS_VARCHAR(src:ORD_BLANKETRELEASE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORD_CREATED), '1900-01-01')) is null and 
                    src:ORD_CREATED is not null) THEN
                    'ORD_CREATED contains a non-timestamp value : \'' || AS_VARCHAR(src:ORD_CREATED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORD_CREDITCARD), '0'), 38, 10) is null and 
                    src:ORD_CREDITCARD is not null) THEN
                    'ORD_CREDITCARD contains a non-numeric value : \'' || AS_VARCHAR(src:ORD_CREDITCARD) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORD_DATE), '1900-01-01')) is null and 
                    src:ORD_DATE is not null) THEN
                    'ORD_DATE contains a non-timestamp value : \'' || AS_VARCHAR(src:ORD_DATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORD_DISCOUNT), '0'), 38, 10) is null and 
                    src:ORD_DISCOUNT is not null) THEN
                    'ORD_DISCOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:ORD_DISCOUNT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORD_DISCPERC), '0'), 38, 10) is null and 
                    src:ORD_DISCPERC is not null) THEN
                    'ORD_DISCPERC contains a non-numeric value : \'' || AS_VARCHAR(src:ORD_DISCPERC) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORD_DUE), '1900-01-01')) is null and 
                    src:ORD_DUE is not null) THEN
                    'ORD_DUE contains a non-timestamp value : \'' || AS_VARCHAR(src:ORD_DUE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORD_EXCH), '0'), 38, 10) is null and 
                    src:ORD_EXCH is not null) THEN
                    'ORD_EXCH contains a non-numeric value : \'' || AS_VARCHAR(src:ORD_EXCH) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORD_EXCHFROMDUAL), '0'), 38, 10) is null and 
                    src:ORD_EXCHFROMDUAL is not null) THEN
                    'ORD_EXCHFROMDUAL contains a non-numeric value : \'' || AS_VARCHAR(src:ORD_EXCHFROMDUAL) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORD_EXCHTODUAL), '0'), 38, 10) is null and 
                    src:ORD_EXCHTODUAL is not null) THEN
                    'ORD_EXCHTODUAL contains a non-numeric value : \'' || AS_VARCHAR(src:ORD_EXCHTODUAL) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORD_INTERFACE), '1900-01-01')) is null and 
                    src:ORD_INTERFACE is not null) THEN
                    'ORD_INTERFACE contains a non-timestamp value : \'' || AS_VARCHAR(src:ORD_INTERFACE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORD_LASTSAVED), '1900-01-01')) is null and 
                    src:ORD_LASTSAVED is not null) THEN
                    'ORD_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:ORD_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORD_LASTSTATUSUPDATE), '1900-01-01')) is null and 
                    src:ORD_LASTSTATUSUPDATE is not null) THEN
                    'ORD_LASTSTATUSUPDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:ORD_LASTSTATUSUPDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORD_PRICE), '0'), 38, 10) is null and 
                    src:ORD_PRICE is not null) THEN
                    'ORD_PRICE contains a non-numeric value : \'' || AS_VARCHAR(src:ORD_PRICE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORD_REVISED), '1900-01-01')) is null and 
                    src:ORD_REVISED is not null) THEN
                    'ORD_REVISED contains a non-timestamp value : \'' || AS_VARCHAR(src:ORD_REVISED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORD_REVISION), '0'), 38, 10) is null and 
                    src:ORD_REVISION is not null) THEN
                    'ORD_REVISION contains a non-numeric value : \'' || AS_VARCHAR(src:ORD_REVISION) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORD_UDFDATE01), '1900-01-01')) is null and 
                    src:ORD_UDFDATE01 is not null) THEN
                    'ORD_UDFDATE01 contains a non-timestamp value : \'' || AS_VARCHAR(src:ORD_UDFDATE01) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORD_UDFDATE02), '1900-01-01')) is null and 
                    src:ORD_UDFDATE02 is not null) THEN
                    'ORD_UDFDATE02 contains a non-timestamp value : \'' || AS_VARCHAR(src:ORD_UDFDATE02) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORD_UDFDATE03), '1900-01-01')) is null and 
                    src:ORD_UDFDATE03 is not null) THEN
                    'ORD_UDFDATE03 contains a non-timestamp value : \'' || AS_VARCHAR(src:ORD_UDFDATE03) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORD_UDFDATE04), '1900-01-01')) is null and 
                    src:ORD_UDFDATE04 is not null) THEN
                    'ORD_UDFDATE04 contains a non-timestamp value : \'' || AS_VARCHAR(src:ORD_UDFDATE04) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORD_UDFDATE05), '1900-01-01')) is null and 
                    src:ORD_UDFDATE05 is not null) THEN
                    'ORD_UDFDATE05 contains a non-timestamp value : \'' || AS_VARCHAR(src:ORD_UDFDATE05) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORD_UDFNUM01), '0'), 38, 10) is null and 
                    src:ORD_UDFNUM01 is not null) THEN
                    'ORD_UDFNUM01 contains a non-numeric value : \'' || AS_VARCHAR(src:ORD_UDFNUM01) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORD_UDFNUM02), '0'), 38, 10) is null and 
                    src:ORD_UDFNUM02 is not null) THEN
                    'ORD_UDFNUM02 contains a non-numeric value : \'' || AS_VARCHAR(src:ORD_UDFNUM02) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORD_UDFNUM03), '0'), 38, 10) is null and 
                    src:ORD_UDFNUM03 is not null) THEN
                    'ORD_UDFNUM03 contains a non-numeric value : \'' || AS_VARCHAR(src:ORD_UDFNUM03) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORD_UDFNUM04), '0'), 38, 10) is null and 
                    src:ORD_UDFNUM04 is not null) THEN
                    'ORD_UDFNUM04 contains a non-numeric value : \'' || AS_VARCHAR(src:ORD_UDFNUM04) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORD_UDFNUM05), '0'), 38, 10) is null and 
                    src:ORD_UDFNUM05 is not null) THEN
                    'ORD_UDFNUM05 contains a non-numeric value : \'' || AS_VARCHAR(src:ORD_UDFNUM05) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORD_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:ORD_UPDATECOUNT is not null) THEN
                    'ORD_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:ORD_UPDATECOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORD_UPDATED), '1900-01-01')) is null and 
                    src:ORD_UPDATED is not null) THEN
                    'ORD_UPDATED contains a non-timestamp value : \'' || AS_VARCHAR(src:ORD_UPDATED) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORD_LASTSAVED), '1900-01-01')) is null and 
                src:ORD_LASTSAVED is not null) THEN
                'ORD_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:ORD_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:ORD_CODE,
                src:ORD_ORG  ORDER BY 
            src:ORD_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5ORDERS as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORD_ACD), '0'), 38, 10) is null and 
                    src:ORD_ACD is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORD_APPROV), '1900-01-01')) is null and 
                    src:ORD_APPROV is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORD_BLANKETRELEASE), '0'), 38, 10) is null and 
                    src:ORD_BLANKETRELEASE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORD_CREATED), '1900-01-01')) is null and 
                    src:ORD_CREATED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORD_CREDITCARD), '0'), 38, 10) is null and 
                    src:ORD_CREDITCARD is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORD_DATE), '1900-01-01')) is null and 
                    src:ORD_DATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORD_DISCOUNT), '0'), 38, 10) is null and 
                    src:ORD_DISCOUNT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORD_DISCPERC), '0'), 38, 10) is null and 
                    src:ORD_DISCPERC is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORD_DUE), '1900-01-01')) is null and 
                    src:ORD_DUE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORD_EXCH), '0'), 38, 10) is null and 
                    src:ORD_EXCH is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORD_EXCHFROMDUAL), '0'), 38, 10) is null and 
                    src:ORD_EXCHFROMDUAL is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORD_EXCHTODUAL), '0'), 38, 10) is null and 
                    src:ORD_EXCHTODUAL is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORD_INTERFACE), '1900-01-01')) is null and 
                    src:ORD_INTERFACE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORD_LASTSAVED), '1900-01-01')) is null and 
                    src:ORD_LASTSAVED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORD_LASTSTATUSUPDATE), '1900-01-01')) is null and 
                    src:ORD_LASTSTATUSUPDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORD_PRICE), '0'), 38, 10) is null and 
                    src:ORD_PRICE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORD_REVISED), '1900-01-01')) is null and 
                    src:ORD_REVISED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORD_REVISION), '0'), 38, 10) is null and 
                    src:ORD_REVISION is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORD_UDFDATE01), '1900-01-01')) is null and 
                    src:ORD_UDFDATE01 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORD_UDFDATE02), '1900-01-01')) is null and 
                    src:ORD_UDFDATE02 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORD_UDFDATE03), '1900-01-01')) is null and 
                    src:ORD_UDFDATE03 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORD_UDFDATE04), '1900-01-01')) is null and 
                    src:ORD_UDFDATE04 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORD_UDFDATE05), '1900-01-01')) is null and 
                    src:ORD_UDFDATE05 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORD_UDFNUM01), '0'), 38, 10) is null and 
                    src:ORD_UDFNUM01 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORD_UDFNUM02), '0'), 38, 10) is null and 
                    src:ORD_UDFNUM02 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORD_UDFNUM03), '0'), 38, 10) is null and 
                    src:ORD_UDFNUM03 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORD_UDFNUM04), '0'), 38, 10) is null and 
                    src:ORD_UDFNUM04 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORD_UDFNUM05), '0'), 38, 10) is null and 
                    src:ORD_UDFNUM05 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORD_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:ORD_UPDATECOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORD_UPDATED), '1900-01-01')) is null and 
                    src:ORD_UPDATED is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORD_LASTSAVED), '1900-01-01')) is null and 
                src:ORD_LASTSAVED is not null)