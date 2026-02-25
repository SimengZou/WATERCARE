CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_CASHIERING_CHARGESETUP_ERROR AS SELECT src, 'IPS_CASHIERING_CHARGESETUP' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCESSID), '0'), 38, 10) is null and 
                    src:ACCESSID is not null) THEN
                    'ACCESSID contains a non-numeric value : \'' || AS_VARCHAR(src:ACCESSID) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BGTNO), '0'), 38, 10) is null and 
                    src:BGTNO is not null) THEN
                    'BGTNO contains a non-numeric value : \'' || AS_VARCHAR(src:BGTNO) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CHGAMT), '0'), 38, 10) is null and 
                    src:CHGAMT is not null) THEN
                    'CHGAMT contains a non-numeric value : \'' || AS_VARCHAR(src:CHGAMT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CONVERSIONFORMULAKEY), '0'), 38, 10) is null and 
                    src:CONVERSIONFORMULAKEY is not null) THEN
                    'CONVERSIONFORMULAKEY contains a non-numeric value : \'' || AS_VARCHAR(src:CONVERSIONFORMULAKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DISPLAYPAGE), '0'), 38, 10) is null and 
                    src:DISPLAYPAGE is not null) THEN
                    'DISPLAYPAGE contains a non-numeric value : \'' || AS_VARCHAR(src:DISPLAYPAGE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EFFDATE), '1900-01-01')) is null and 
                    src:EFFDATE is not null) THEN
                    'EFFDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:EFFDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EXPDATE), '1900-01-01')) is null and 
                    src:EXPDATE is not null) THEN
                    'EXPDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:EXPDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYMENTFORMULA), '0'), 38, 10) is null and 
                    src:PAYMENTFORMULA is not null) THEN
                    'PAYMENTFORMULA contains a non-numeric value : \'' || AS_VARCHAR(src:PAYMENTFORMULA) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REVERSALFORMULA), '0'), 38, 10) is null and 
                    src:REVERSALFORMULA is not null) THEN
                    'REVERSALFORMULA contains a non-numeric value : \'' || AS_VARCHAR(src:REVERSALFORMULA) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SEARCHPAGE), '0'), 38, 10) is null and 
                    src:SEARCHPAGE is not null) THEN
                    'SEARCHPAGE contains a non-numeric value : \'' || AS_VARCHAR(src:SEARCHPAGE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TAXRATE), '0'), 38, 10) is null and 
                    src:TAXRATE is not null) THEN
                    'TAXRATE contains a non-numeric value : \'' || AS_VARCHAR(src:TAXRATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TENDERALLOWED), '0'), 38, 10) is null and 
                    src:TENDERALLOWED is not null) THEN
                    'TENDERALLOWED contains a non-numeric value : \'' || AS_VARCHAR(src:TENDERALLOWED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) THEN
                    'VARIATION_ID contains a non-numeric value : \'' || AS_VARCHAR(src:VARIATION_ID) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null) THEN
                'VARIATION_ID contains a non-timestamp value : \'' || AS_VARCHAR(src:VARIATION_ID) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:CHARGETYPE  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_CASHIERING_CHARGESETUP as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCESSID), '0'), 38, 10) is null and 
                    src:ACCESSID is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BGTNO), '0'), 38, 10) is null and 
                    src:BGTNO is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CHGAMT), '0'), 38, 10) is null and 
                    src:CHGAMT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CONVERSIONFORMULAKEY), '0'), 38, 10) is null and 
                    src:CONVERSIONFORMULAKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DISPLAYPAGE), '0'), 38, 10) is null and 
                    src:DISPLAYPAGE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EFFDATE), '1900-01-01')) is null and 
                    src:EFFDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EXPDATE), '1900-01-01')) is null and 
                    src:EXPDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYMENTFORMULA), '0'), 38, 10) is null and 
                    src:PAYMENTFORMULA is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REVERSALFORMULA), '0'), 38, 10) is null and 
                    src:REVERSALFORMULA is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SEARCHPAGE), '0'), 38, 10) is null and 
                    src:SEARCHPAGE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TAXRATE), '0'), 38, 10) is null and 
                    src:TAXRATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TENDERALLOWED), '0'), 38, 10) is null and 
                    src:TENDERALLOWED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)