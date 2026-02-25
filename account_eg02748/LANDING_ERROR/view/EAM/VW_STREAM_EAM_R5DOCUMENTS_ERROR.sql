CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5DOCUMENTS_ERROR AS SELECT src, 'EAM_R5DOCUMENTS' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DOC_DATEEFFECTIVE), '1900-01-01')) is null and 
                    src:DOC_DATEEFFECTIVE is not null) THEN
                    'DOC_DATEEFFECTIVE contains a non-timestamp value : \'' || AS_VARCHAR(src:DOC_DATEEFFECTIVE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DOC_DATEEXPIRED), '1900-01-01')) is null and 
                    src:DOC_DATEEXPIRED is not null) THEN
                    'DOC_DATEEXPIRED contains a non-timestamp value : \'' || AS_VARCHAR(src:DOC_DATEEXPIRED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DOC_DMACHARGE), '0'), 38, 10) is null and 
                    src:DOC_DMACHARGE is not null) THEN
                    'DOC_DMACHARGE contains a non-numeric value : \'' || AS_VARCHAR(src:DOC_DMACHARGE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DOC_DURATION), '0'), 38, 10) is null and 
                    src:DOC_DURATION is not null) THEN
                    'DOC_DURATION contains a non-numeric value : \'' || AS_VARCHAR(src:DOC_DURATION) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DOC_EXPIR), '1900-01-01')) is null and 
                    src:DOC_EXPIR is not null) THEN
                    'DOC_EXPIR contains a non-timestamp value : \'' || AS_VARCHAR(src:DOC_EXPIR) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DOC_FILESIZE), '0'), 38, 10) is null and 
                    src:DOC_FILESIZE is not null) THEN
                    'DOC_FILESIZE contains a non-numeric value : \'' || AS_VARCHAR(src:DOC_FILESIZE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DOC_FIXEDLABORAMOUNT), '0'), 38, 10) is null and 
                    src:DOC_FIXEDLABORAMOUNT is not null) THEN
                    'DOC_FIXEDLABORAMOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:DOC_FIXEDLABORAMOUNT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DOC_FIXEDLABORRATE), '0'), 38, 10) is null and 
                    src:DOC_FIXEDLABORRATE is not null) THEN
                    'DOC_FIXEDLABORRATE contains a non-numeric value : \'' || AS_VARCHAR(src:DOC_FIXEDLABORRATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DOC_FIXEDSTOCKAMOUNT), '0'), 38, 10) is null and 
                    src:DOC_FIXEDSTOCKAMOUNT is not null) THEN
                    'DOC_FIXEDSTOCKAMOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:DOC_FIXEDSTOCKAMOUNT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DOC_HIRCHARGE), '0'), 38, 10) is null and 
                    src:DOC_HIRCHARGE is not null) THEN
                    'DOC_HIRCHARGE contains a non-numeric value : \'' || AS_VARCHAR(src:DOC_HIRCHARGE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DOC_LABCHARGE), '0'), 38, 10) is null and 
                    src:DOC_LABCHARGE is not null) THEN
                    'DOC_LABCHARGE contains a non-numeric value : \'' || AS_VARCHAR(src:DOC_LABCHARGE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DOC_LASTSAVED), '1900-01-01')) is null and 
                    src:DOC_LASTSAVED is not null) THEN
                    'DOC_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:DOC_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DOC_MATCHARGE), '0'), 38, 10) is null and 
                    src:DOC_MATCHARGE is not null) THEN
                    'DOC_MATCHARGE contains a non-numeric value : \'' || AS_VARCHAR(src:DOC_MATCHARGE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DOC_MAXLABORAMOUNT), '0'), 38, 10) is null and 
                    src:DOC_MAXLABORAMOUNT is not null) THEN
                    'DOC_MAXLABORAMOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:DOC_MAXLABORAMOUNT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DOC_MAXSTOCKAMOUNT), '0'), 38, 10) is null and 
                    src:DOC_MAXSTOCKAMOUNT is not null) THEN
                    'DOC_MAXSTOCKAMOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:DOC_MAXSTOCKAMOUNT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DOC_PAGES), '0'), 38, 10) is null and 
                    src:DOC_PAGES is not null) THEN
                    'DOC_PAGES contains a non-numeric value : \'' || AS_VARCHAR(src:DOC_PAGES) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DOC_REVISION), '1900-01-01')) is null and 
                    src:DOC_REVISION is not null) THEN
                    'DOC_REVISION contains a non-timestamp value : \'' || AS_VARCHAR(src:DOC_REVISION) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DOC_SERVICECHARGE), '0'), 38, 10) is null and 
                    src:DOC_SERVICECHARGE is not null) THEN
                    'DOC_SERVICECHARGE contains a non-numeric value : \'' || AS_VARCHAR(src:DOC_SERVICECHARGE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DOC_THRESHOLD), '0'), 38, 10) is null and 
                    src:DOC_THRESHOLD is not null) THEN
                    'DOC_THRESHOLD contains a non-numeric value : \'' || AS_VARCHAR(src:DOC_THRESHOLD) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DOC_TOOLCHARGE), '0'), 38, 10) is null and 
                    src:DOC_TOOLCHARGE is not null) THEN
                    'DOC_TOOLCHARGE contains a non-numeric value : \'' || AS_VARCHAR(src:DOC_TOOLCHARGE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DOC_UDFDATE01), '1900-01-01')) is null and 
                    src:DOC_UDFDATE01 is not null) THEN
                    'DOC_UDFDATE01 contains a non-timestamp value : \'' || AS_VARCHAR(src:DOC_UDFDATE01) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DOC_UDFDATE02), '1900-01-01')) is null and 
                    src:DOC_UDFDATE02 is not null) THEN
                    'DOC_UDFDATE02 contains a non-timestamp value : \'' || AS_VARCHAR(src:DOC_UDFDATE02) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DOC_UDFDATE03), '1900-01-01')) is null and 
                    src:DOC_UDFDATE03 is not null) THEN
                    'DOC_UDFDATE03 contains a non-timestamp value : \'' || AS_VARCHAR(src:DOC_UDFDATE03) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DOC_UDFDATE04), '1900-01-01')) is null and 
                    src:DOC_UDFDATE04 is not null) THEN
                    'DOC_UDFDATE04 contains a non-timestamp value : \'' || AS_VARCHAR(src:DOC_UDFDATE04) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DOC_UDFDATE05), '1900-01-01')) is null and 
                    src:DOC_UDFDATE05 is not null) THEN
                    'DOC_UDFDATE05 contains a non-timestamp value : \'' || AS_VARCHAR(src:DOC_UDFDATE05) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DOC_UDFNUM01), '0'), 38, 10) is null and 
                    src:DOC_UDFNUM01 is not null) THEN
                    'DOC_UDFNUM01 contains a non-numeric value : \'' || AS_VARCHAR(src:DOC_UDFNUM01) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DOC_UDFNUM02), '0'), 38, 10) is null and 
                    src:DOC_UDFNUM02 is not null) THEN
                    'DOC_UDFNUM02 contains a non-numeric value : \'' || AS_VARCHAR(src:DOC_UDFNUM02) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DOC_UDFNUM03), '0'), 38, 10) is null and 
                    src:DOC_UDFNUM03 is not null) THEN
                    'DOC_UDFNUM03 contains a non-numeric value : \'' || AS_VARCHAR(src:DOC_UDFNUM03) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DOC_UDFNUM04), '0'), 38, 10) is null and 
                    src:DOC_UDFNUM04 is not null) THEN
                    'DOC_UDFNUM04 contains a non-numeric value : \'' || AS_VARCHAR(src:DOC_UDFNUM04) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DOC_UDFNUM05), '0'), 38, 10) is null and 
                    src:DOC_UDFNUM05 is not null) THEN
                    'DOC_UDFNUM05 contains a non-numeric value : \'' || AS_VARCHAR(src:DOC_UDFNUM05) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DOC_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:DOC_UPDATECOUNT is not null) THEN
                    'DOC_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:DOC_UPDATECOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DOC_UPLOADED), '1900-01-01')) is null and 
                    src:DOC_UPLOADED is not null) THEN
                    'DOC_UPLOADED contains a non-timestamp value : \'' || AS_VARCHAR(src:DOC_UPLOADED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DOC_WARSTART), '1900-01-01')) is null and 
                    src:DOC_WARSTART is not null) THEN
                    'DOC_WARSTART contains a non-timestamp value : \'' || AS_VARCHAR(src:DOC_WARSTART) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DOC_LASTSAVED), '1900-01-01')) is null and 
                src:DOC_LASTSAVED is not null) THEN
                'DOC_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:DOC_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:DOC_CODE  ORDER BY 
            src:DOC_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5DOCUMENTS as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DOC_DATEEFFECTIVE), '1900-01-01')) is null and 
                    src:DOC_DATEEFFECTIVE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DOC_DATEEXPIRED), '1900-01-01')) is null and 
                    src:DOC_DATEEXPIRED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DOC_DMACHARGE), '0'), 38, 10) is null and 
                    src:DOC_DMACHARGE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DOC_DURATION), '0'), 38, 10) is null and 
                    src:DOC_DURATION is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DOC_EXPIR), '1900-01-01')) is null and 
                    src:DOC_EXPIR is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DOC_FILESIZE), '0'), 38, 10) is null and 
                    src:DOC_FILESIZE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DOC_FIXEDLABORAMOUNT), '0'), 38, 10) is null and 
                    src:DOC_FIXEDLABORAMOUNT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DOC_FIXEDLABORRATE), '0'), 38, 10) is null and 
                    src:DOC_FIXEDLABORRATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DOC_FIXEDSTOCKAMOUNT), '0'), 38, 10) is null and 
                    src:DOC_FIXEDSTOCKAMOUNT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DOC_HIRCHARGE), '0'), 38, 10) is null and 
                    src:DOC_HIRCHARGE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DOC_LABCHARGE), '0'), 38, 10) is null and 
                    src:DOC_LABCHARGE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DOC_LASTSAVED), '1900-01-01')) is null and 
                    src:DOC_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DOC_MATCHARGE), '0'), 38, 10) is null and 
                    src:DOC_MATCHARGE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DOC_MAXLABORAMOUNT), '0'), 38, 10) is null and 
                    src:DOC_MAXLABORAMOUNT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DOC_MAXSTOCKAMOUNT), '0'), 38, 10) is null and 
                    src:DOC_MAXSTOCKAMOUNT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DOC_PAGES), '0'), 38, 10) is null and 
                    src:DOC_PAGES is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DOC_REVISION), '1900-01-01')) is null and 
                    src:DOC_REVISION is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DOC_SERVICECHARGE), '0'), 38, 10) is null and 
                    src:DOC_SERVICECHARGE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DOC_THRESHOLD), '0'), 38, 10) is null and 
                    src:DOC_THRESHOLD is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DOC_TOOLCHARGE), '0'), 38, 10) is null and 
                    src:DOC_TOOLCHARGE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DOC_UDFDATE01), '1900-01-01')) is null and 
                    src:DOC_UDFDATE01 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DOC_UDFDATE02), '1900-01-01')) is null and 
                    src:DOC_UDFDATE02 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DOC_UDFDATE03), '1900-01-01')) is null and 
                    src:DOC_UDFDATE03 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DOC_UDFDATE04), '1900-01-01')) is null and 
                    src:DOC_UDFDATE04 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DOC_UDFDATE05), '1900-01-01')) is null and 
                    src:DOC_UDFDATE05 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DOC_UDFNUM01), '0'), 38, 10) is null and 
                    src:DOC_UDFNUM01 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DOC_UDFNUM02), '0'), 38, 10) is null and 
                    src:DOC_UDFNUM02 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DOC_UDFNUM03), '0'), 38, 10) is null and 
                    src:DOC_UDFNUM03 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DOC_UDFNUM04), '0'), 38, 10) is null and 
                    src:DOC_UDFNUM04 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DOC_UDFNUM05), '0'), 38, 10) is null and 
                    src:DOC_UDFNUM05 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DOC_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:DOC_UPDATECOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DOC_UPLOADED), '1900-01-01')) is null and 
                    src:DOC_UPLOADED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DOC_WARSTART), '1900-01-01')) is null and 
                    src:DOC_WARSTART is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DOC_LASTSAVED), '1900-01-01')) is null and 
                src:DOC_LASTSAVED is not null)