CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5BOOKEDHOURS_ERROR AS SELECT src, 'EAM_R5BOOKEDHOURS' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BOO_ACD), '0'), 38, 10) is null and 
                    src:BOO_ACD is not null) THEN
                    'BOO_ACD contains a non-numeric value : \'' || AS_VARCHAR(src:BOO_ACD) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BOO_ACT), '0'), 38, 10) is null and 
                    src:BOO_ACT is not null) THEN
                    'BOO_ACT contains a non-numeric value : \'' || AS_VARCHAR(src:BOO_ACT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BOO_CORRECTIONDATE), '1900-01-01')) is null and 
                    src:BOO_CORRECTIONDATE is not null) THEN
                    'BOO_CORRECTIONDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:BOO_CORRECTIONDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BOO_COST), '0'), 38, 10) is null and 
                    src:BOO_COST is not null) THEN
                    'BOO_COST contains a non-numeric value : \'' || AS_VARCHAR(src:BOO_COST) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BOO_DATE), '1900-01-01')) is null and 
                    src:BOO_DATE is not null) THEN
                    'BOO_DATE contains a non-timestamp value : \'' || AS_VARCHAR(src:BOO_DATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BOO_ENTERED), '1900-01-01')) is null and 
                    src:BOO_ENTERED is not null) THEN
                    'BOO_ENTERED contains a non-timestamp value : \'' || AS_VARCHAR(src:BOO_ENTERED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BOO_FLEETMARKUP), '0'), 38, 10) is null and 
                    src:BOO_FLEETMARKUP is not null) THEN
                    'BOO_FLEETMARKUP contains a non-numeric value : \'' || AS_VARCHAR(src:BOO_FLEETMARKUP) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BOO_GLTRANSFER), '1900-01-01')) is null and 
                    src:BOO_GLTRANSFER is not null) THEN
                    'BOO_GLTRANSFER contains a non-timestamp value : \'' || AS_VARCHAR(src:BOO_GLTRANSFER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BOO_HOURS), '0'), 38, 10) is null and 
                    src:BOO_HOURS is not null) THEN
                    'BOO_HOURS contains a non-numeric value : \'' || AS_VARCHAR(src:BOO_HOURS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BOO_INVOICELINE), '0'), 38, 10) is null and 
                    src:BOO_INVOICELINE is not null) THEN
                    'BOO_INVOICELINE contains a non-numeric value : \'' || AS_VARCHAR(src:BOO_INVOICELINE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BOO_INVOICE_REVISION), '0'), 38, 10) is null and 
                    src:BOO_INVOICE_REVISION is not null) THEN
                    'BOO_INVOICE_REVISION contains a non-numeric value : \'' || AS_VARCHAR(src:BOO_INVOICE_REVISION) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BOO_LASTSAVED), '1900-01-01')) is null and 
                    src:BOO_LASTSAVED is not null) THEN
                    'BOO_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:BOO_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BOO_OFF), '0'), 38, 10) is null and 
                    src:BOO_OFF is not null) THEN
                    'BOO_OFF contains a non-numeric value : \'' || AS_VARCHAR(src:BOO_OFF) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BOO_ON), '0'), 38, 10) is null and 
                    src:BOO_ON is not null) THEN
                    'BOO_ON contains a non-numeric value : \'' || AS_VARCHAR(src:BOO_ON) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BOO_ORDLINE), '0'), 38, 10) is null and 
                    src:BOO_ORDLINE is not null) THEN
                    'BOO_ORDLINE contains a non-numeric value : \'' || AS_VARCHAR(src:BOO_ORDLINE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BOO_ORIGHOURS), '0'), 38, 10) is null and 
                    src:BOO_ORIGHOURS is not null) THEN
                    'BOO_ORIGHOURS contains a non-numeric value : \'' || AS_VARCHAR(src:BOO_ORIGHOURS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BOO_ORIGTAXAMOUNT), '0'), 38, 10) is null and 
                    src:BOO_ORIGTAXAMOUNT is not null) THEN
                    'BOO_ORIGTAXAMOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:BOO_ORIGTAXAMOUNT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BOO_RATE), '0'), 38, 10) is null and 
                    src:BOO_RATE is not null) THEN
                    'BOO_RATE contains a non-numeric value : \'' || AS_VARCHAR(src:BOO_RATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BOO_TAXAMOUNT), '0'), 38, 10) is null and 
                    src:BOO_TAXAMOUNT is not null) THEN
                    'BOO_TAXAMOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:BOO_TAXAMOUNT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BOO_TRANSGROUP), '0'), 38, 10) is null and 
                    src:BOO_TRANSGROUP is not null) THEN
                    'BOO_TRANSGROUP contains a non-numeric value : \'' || AS_VARCHAR(src:BOO_TRANSGROUP) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BOO_TRANSORGID), '0'), 38, 10) is null and 
                    src:BOO_TRANSORGID is not null) THEN
                    'BOO_TRANSORGID contains a non-numeric value : \'' || AS_VARCHAR(src:BOO_TRANSORGID) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BOO_UDFDATE01), '1900-01-01')) is null and 
                    src:BOO_UDFDATE01 is not null) THEN
                    'BOO_UDFDATE01 contains a non-timestamp value : \'' || AS_VARCHAR(src:BOO_UDFDATE01) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BOO_UDFDATE02), '1900-01-01')) is null and 
                    src:BOO_UDFDATE02 is not null) THEN
                    'BOO_UDFDATE02 contains a non-timestamp value : \'' || AS_VARCHAR(src:BOO_UDFDATE02) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BOO_UDFDATE03), '1900-01-01')) is null and 
                    src:BOO_UDFDATE03 is not null) THEN
                    'BOO_UDFDATE03 contains a non-timestamp value : \'' || AS_VARCHAR(src:BOO_UDFDATE03) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BOO_UDFDATE04), '1900-01-01')) is null and 
                    src:BOO_UDFDATE04 is not null) THEN
                    'BOO_UDFDATE04 contains a non-timestamp value : \'' || AS_VARCHAR(src:BOO_UDFDATE04) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BOO_UDFDATE05), '1900-01-01')) is null and 
                    src:BOO_UDFDATE05 is not null) THEN
                    'BOO_UDFDATE05 contains a non-timestamp value : \'' || AS_VARCHAR(src:BOO_UDFDATE05) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BOO_UDFNUM01), '0'), 38, 10) is null and 
                    src:BOO_UDFNUM01 is not null) THEN
                    'BOO_UDFNUM01 contains a non-numeric value : \'' || AS_VARCHAR(src:BOO_UDFNUM01) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BOO_UDFNUM02), '0'), 38, 10) is null and 
                    src:BOO_UDFNUM02 is not null) THEN
                    'BOO_UDFNUM02 contains a non-numeric value : \'' || AS_VARCHAR(src:BOO_UDFNUM02) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BOO_UDFNUM03), '0'), 38, 10) is null and 
                    src:BOO_UDFNUM03 is not null) THEN
                    'BOO_UDFNUM03 contains a non-numeric value : \'' || AS_VARCHAR(src:BOO_UDFNUM03) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BOO_UDFNUM04), '0'), 38, 10) is null and 
                    src:BOO_UDFNUM04 is not null) THEN
                    'BOO_UDFNUM04 contains a non-numeric value : \'' || AS_VARCHAR(src:BOO_UDFNUM04) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BOO_UDFNUM05), '0'), 38, 10) is null and 
                    src:BOO_UDFNUM05 is not null) THEN
                    'BOO_UDFNUM05 contains a non-numeric value : \'' || AS_VARCHAR(src:BOO_UDFNUM05) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BOO_UNMATCHEDINVOICELINE), '0'), 38, 10) is null and 
                    src:BOO_UNMATCHEDINVOICELINE is not null) THEN
                    'BOO_UNMATCHEDINVOICELINE contains a non-numeric value : \'' || AS_VARCHAR(src:BOO_UNMATCHEDINVOICELINE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BOO_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:BOO_UPDATECOUNT is not null) THEN
                    'BOO_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:BOO_UPDATECOUNT) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BOO_LASTSAVED), '1900-01-01')) is null and 
                src:BOO_LASTSAVED is not null) THEN
                'BOO_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:BOO_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:BOO_CODE  ORDER BY 
            src:BOO_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5BOOKEDHOURS as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BOO_ACD), '0'), 38, 10) is null and 
                    src:BOO_ACD is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BOO_ACT), '0'), 38, 10) is null and 
                    src:BOO_ACT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BOO_CORRECTIONDATE), '1900-01-01')) is null and 
                    src:BOO_CORRECTIONDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BOO_COST), '0'), 38, 10) is null and 
                    src:BOO_COST is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BOO_DATE), '1900-01-01')) is null and 
                    src:BOO_DATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BOO_ENTERED), '1900-01-01')) is null and 
                    src:BOO_ENTERED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BOO_FLEETMARKUP), '0'), 38, 10) is null and 
                    src:BOO_FLEETMARKUP is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BOO_GLTRANSFER), '1900-01-01')) is null and 
                    src:BOO_GLTRANSFER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BOO_HOURS), '0'), 38, 10) is null and 
                    src:BOO_HOURS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BOO_INVOICELINE), '0'), 38, 10) is null and 
                    src:BOO_INVOICELINE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BOO_INVOICE_REVISION), '0'), 38, 10) is null and 
                    src:BOO_INVOICE_REVISION is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BOO_LASTSAVED), '1900-01-01')) is null and 
                    src:BOO_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BOO_OFF), '0'), 38, 10) is null and 
                    src:BOO_OFF is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BOO_ON), '0'), 38, 10) is null and 
                    src:BOO_ON is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BOO_ORDLINE), '0'), 38, 10) is null and 
                    src:BOO_ORDLINE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BOO_ORIGHOURS), '0'), 38, 10) is null and 
                    src:BOO_ORIGHOURS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BOO_ORIGTAXAMOUNT), '0'), 38, 10) is null and 
                    src:BOO_ORIGTAXAMOUNT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BOO_RATE), '0'), 38, 10) is null and 
                    src:BOO_RATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BOO_TAXAMOUNT), '0'), 38, 10) is null and 
                    src:BOO_TAXAMOUNT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BOO_TRANSGROUP), '0'), 38, 10) is null and 
                    src:BOO_TRANSGROUP is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BOO_TRANSORGID), '0'), 38, 10) is null and 
                    src:BOO_TRANSORGID is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BOO_UDFDATE01), '1900-01-01')) is null and 
                    src:BOO_UDFDATE01 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BOO_UDFDATE02), '1900-01-01')) is null and 
                    src:BOO_UDFDATE02 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BOO_UDFDATE03), '1900-01-01')) is null and 
                    src:BOO_UDFDATE03 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BOO_UDFDATE04), '1900-01-01')) is null and 
                    src:BOO_UDFDATE04 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BOO_UDFDATE05), '1900-01-01')) is null and 
                    src:BOO_UDFDATE05 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BOO_UDFNUM01), '0'), 38, 10) is null and 
                    src:BOO_UDFNUM01 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BOO_UDFNUM02), '0'), 38, 10) is null and 
                    src:BOO_UDFNUM02 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BOO_UDFNUM03), '0'), 38, 10) is null and 
                    src:BOO_UDFNUM03 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BOO_UDFNUM04), '0'), 38, 10) is null and 
                    src:BOO_UDFNUM04 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BOO_UDFNUM05), '0'), 38, 10) is null and 
                    src:BOO_UDFNUM05 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BOO_UNMATCHEDINVOICELINE), '0'), 38, 10) is null and 
                    src:BOO_UNMATCHEDINVOICELINE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BOO_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:BOO_UPDATECOUNT is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BOO_LASTSAVED), '1900-01-01')) is null and 
                src:BOO_LASTSAVED is not null)