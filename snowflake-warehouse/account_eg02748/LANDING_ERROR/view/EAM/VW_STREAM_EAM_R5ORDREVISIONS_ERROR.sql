CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5ORDREVISIONS_ERROR AS SELECT src, 'EAM_R5ORDREVISIONS' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORR_ACD), '0'), 38, 10) is null and 
                    src:ORR_ACD is not null) THEN
                    'ORR_ACD contains a non-numeric value : \'' || AS_VARCHAR(src:ORR_ACD) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORR_APPROV), '1900-01-01')) is null and 
                    src:ORR_APPROV is not null) THEN
                    'ORR_APPROV contains a non-timestamp value : \'' || AS_VARCHAR(src:ORR_APPROV) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORR_BLANKETRELEASE), '0'), 38, 10) is null and 
                    src:ORR_BLANKETRELEASE is not null) THEN
                    'ORR_BLANKETRELEASE contains a non-numeric value : \'' || AS_VARCHAR(src:ORR_BLANKETRELEASE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORR_CREATED), '1900-01-01')) is null and 
                    src:ORR_CREATED is not null) THEN
                    'ORR_CREATED contains a non-timestamp value : \'' || AS_VARCHAR(src:ORR_CREATED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORR_CREDITCARD), '0'), 38, 10) is null and 
                    src:ORR_CREDITCARD is not null) THEN
                    'ORR_CREDITCARD contains a non-numeric value : \'' || AS_VARCHAR(src:ORR_CREDITCARD) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORR_DATE), '1900-01-01')) is null and 
                    src:ORR_DATE is not null) THEN
                    'ORR_DATE contains a non-timestamp value : \'' || AS_VARCHAR(src:ORR_DATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORR_DISCOUNT), '0'), 38, 10) is null and 
                    src:ORR_DISCOUNT is not null) THEN
                    'ORR_DISCOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:ORR_DISCOUNT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORR_DISCPERC), '0'), 38, 10) is null and 
                    src:ORR_DISCPERC is not null) THEN
                    'ORR_DISCPERC contains a non-numeric value : \'' || AS_VARCHAR(src:ORR_DISCPERC) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORR_DUE), '1900-01-01')) is null and 
                    src:ORR_DUE is not null) THEN
                    'ORR_DUE contains a non-timestamp value : \'' || AS_VARCHAR(src:ORR_DUE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORR_EXCH), '0'), 38, 10) is null and 
                    src:ORR_EXCH is not null) THEN
                    'ORR_EXCH contains a non-numeric value : \'' || AS_VARCHAR(src:ORR_EXCH) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORR_EXCHFROMDUAL), '0'), 38, 10) is null and 
                    src:ORR_EXCHFROMDUAL is not null) THEN
                    'ORR_EXCHFROMDUAL contains a non-numeric value : \'' || AS_VARCHAR(src:ORR_EXCHFROMDUAL) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORR_EXCHTODUAL), '0'), 38, 10) is null and 
                    src:ORR_EXCHTODUAL is not null) THEN
                    'ORR_EXCHTODUAL contains a non-numeric value : \'' || AS_VARCHAR(src:ORR_EXCHTODUAL) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORR_INTERFACE), '1900-01-01')) is null and 
                    src:ORR_INTERFACE is not null) THEN
                    'ORR_INTERFACE contains a non-timestamp value : \'' || AS_VARCHAR(src:ORR_INTERFACE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORR_LASTSAVED), '1900-01-01')) is null and 
                    src:ORR_LASTSAVED is not null) THEN
                    'ORR_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:ORR_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORR_LASTSTATUSUPDATE), '1900-01-01')) is null and 
                    src:ORR_LASTSTATUSUPDATE is not null) THEN
                    'ORR_LASTSTATUSUPDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:ORR_LASTSTATUSUPDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORR_PRICE), '0'), 38, 10) is null and 
                    src:ORR_PRICE is not null) THEN
                    'ORR_PRICE contains a non-numeric value : \'' || AS_VARCHAR(src:ORR_PRICE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORR_REVISED), '1900-01-01')) is null and 
                    src:ORR_REVISED is not null) THEN
                    'ORR_REVISED contains a non-timestamp value : \'' || AS_VARCHAR(src:ORR_REVISED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORR_REVISION), '0'), 38, 10) is null and 
                    src:ORR_REVISION is not null) THEN
                    'ORR_REVISION contains a non-numeric value : \'' || AS_VARCHAR(src:ORR_REVISION) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORR_UDFDATE01), '1900-01-01')) is null and 
                    src:ORR_UDFDATE01 is not null) THEN
                    'ORR_UDFDATE01 contains a non-timestamp value : \'' || AS_VARCHAR(src:ORR_UDFDATE01) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORR_UDFDATE02), '1900-01-01')) is null and 
                    src:ORR_UDFDATE02 is not null) THEN
                    'ORR_UDFDATE02 contains a non-timestamp value : \'' || AS_VARCHAR(src:ORR_UDFDATE02) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORR_UDFDATE03), '1900-01-01')) is null and 
                    src:ORR_UDFDATE03 is not null) THEN
                    'ORR_UDFDATE03 contains a non-timestamp value : \'' || AS_VARCHAR(src:ORR_UDFDATE03) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORR_UDFDATE04), '1900-01-01')) is null and 
                    src:ORR_UDFDATE04 is not null) THEN
                    'ORR_UDFDATE04 contains a non-timestamp value : \'' || AS_VARCHAR(src:ORR_UDFDATE04) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORR_UDFDATE05), '1900-01-01')) is null and 
                    src:ORR_UDFDATE05 is not null) THEN
                    'ORR_UDFDATE05 contains a non-timestamp value : \'' || AS_VARCHAR(src:ORR_UDFDATE05) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORR_UDFNUM01), '0'), 38, 10) is null and 
                    src:ORR_UDFNUM01 is not null) THEN
                    'ORR_UDFNUM01 contains a non-numeric value : \'' || AS_VARCHAR(src:ORR_UDFNUM01) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORR_UDFNUM02), '0'), 38, 10) is null and 
                    src:ORR_UDFNUM02 is not null) THEN
                    'ORR_UDFNUM02 contains a non-numeric value : \'' || AS_VARCHAR(src:ORR_UDFNUM02) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORR_UDFNUM03), '0'), 38, 10) is null and 
                    src:ORR_UDFNUM03 is not null) THEN
                    'ORR_UDFNUM03 contains a non-numeric value : \'' || AS_VARCHAR(src:ORR_UDFNUM03) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORR_UDFNUM04), '0'), 38, 10) is null and 
                    src:ORR_UDFNUM04 is not null) THEN
                    'ORR_UDFNUM04 contains a non-numeric value : \'' || AS_VARCHAR(src:ORR_UDFNUM04) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORR_UDFNUM05), '0'), 38, 10) is null and 
                    src:ORR_UDFNUM05 is not null) THEN
                    'ORR_UDFNUM05 contains a non-numeric value : \'' || AS_VARCHAR(src:ORR_UDFNUM05) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORR_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:ORR_UPDATECOUNT is not null) THEN
                    'ORR_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:ORR_UPDATECOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORR_UPDATED), '1900-01-01')) is null and 
                    src:ORR_UPDATED is not null) THEN
                    'ORR_UPDATED contains a non-timestamp value : \'' || AS_VARCHAR(src:ORR_UPDATED) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORR_LASTSAVED), '1900-01-01')) is null and 
                src:ORR_LASTSAVED is not null) THEN
                'ORR_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:ORR_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:ORR_ORDER,
                src:ORR_ORDER_ORG,
                src:ORR_REVISION  ORDER BY 
            src:ORR_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5ORDREVISIONS as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORR_ACD), '0'), 38, 10) is null and 
                    src:ORR_ACD is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORR_APPROV), '1900-01-01')) is null and 
                    src:ORR_APPROV is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORR_BLANKETRELEASE), '0'), 38, 10) is null and 
                    src:ORR_BLANKETRELEASE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORR_CREATED), '1900-01-01')) is null and 
                    src:ORR_CREATED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORR_CREDITCARD), '0'), 38, 10) is null and 
                    src:ORR_CREDITCARD is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORR_DATE), '1900-01-01')) is null and 
                    src:ORR_DATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORR_DISCOUNT), '0'), 38, 10) is null and 
                    src:ORR_DISCOUNT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORR_DISCPERC), '0'), 38, 10) is null and 
                    src:ORR_DISCPERC is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORR_DUE), '1900-01-01')) is null and 
                    src:ORR_DUE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORR_EXCH), '0'), 38, 10) is null and 
                    src:ORR_EXCH is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORR_EXCHFROMDUAL), '0'), 38, 10) is null and 
                    src:ORR_EXCHFROMDUAL is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORR_EXCHTODUAL), '0'), 38, 10) is null and 
                    src:ORR_EXCHTODUAL is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORR_INTERFACE), '1900-01-01')) is null and 
                    src:ORR_INTERFACE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORR_LASTSAVED), '1900-01-01')) is null and 
                    src:ORR_LASTSAVED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORR_LASTSTATUSUPDATE), '1900-01-01')) is null and 
                    src:ORR_LASTSTATUSUPDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORR_PRICE), '0'), 38, 10) is null and 
                    src:ORR_PRICE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORR_REVISED), '1900-01-01')) is null and 
                    src:ORR_REVISED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORR_REVISION), '0'), 38, 10) is null and 
                    src:ORR_REVISION is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORR_UDFDATE01), '1900-01-01')) is null and 
                    src:ORR_UDFDATE01 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORR_UDFDATE02), '1900-01-01')) is null and 
                    src:ORR_UDFDATE02 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORR_UDFDATE03), '1900-01-01')) is null and 
                    src:ORR_UDFDATE03 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORR_UDFDATE04), '1900-01-01')) is null and 
                    src:ORR_UDFDATE04 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORR_UDFDATE05), '1900-01-01')) is null and 
                    src:ORR_UDFDATE05 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORR_UDFNUM01), '0'), 38, 10) is null and 
                    src:ORR_UDFNUM01 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORR_UDFNUM02), '0'), 38, 10) is null and 
                    src:ORR_UDFNUM02 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORR_UDFNUM03), '0'), 38, 10) is null and 
                    src:ORR_UDFNUM03 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORR_UDFNUM04), '0'), 38, 10) is null and 
                    src:ORR_UDFNUM04 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORR_UDFNUM05), '0'), 38, 10) is null and 
                    src:ORR_UDFNUM05 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORR_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:ORR_UPDATECOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORR_UPDATED), '1900-01-01')) is null and 
                    src:ORR_UPDATED is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORR_LASTSAVED), '1900-01-01')) is null and 
                src:ORR_LASTSAVED is not null)