CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_BILLING_ACCTCNTC_ERROR AS SELECT src, 'IPS_BILLING_ACCTCNTC' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTCONTACTKEY), '0'), 38, 10) is null and 
                    src:ACCOUNTCONTACTKEY is not null) THEN
                    'ACCOUNTCONTACTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ACCOUNTCONTACTKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTKEY), '0'), 38, 10) is null and 
                    src:ACCOUNTKEY is not null) THEN
                    'ACCOUNTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ACCOUNTKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CAPFRDTTM), '1900-01-01')) is null and 
                    src:CAPFRDTTM is not null) THEN
                    'CAPFRDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:CAPFRDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CAPTODTTM), '1900-01-01')) is null and 
                    src:CAPTODTTM is not null) THEN
                    'CAPTODTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:CAPTODTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CNTCTKEY), '0'), 38, 10) is null and 
                    src:CNTCTKEY is not null) THEN
                    'CNTCTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:CNTCTKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ENTEFFDATE), '1900-01-01')) is null and 
                    src:ENTEFFDATE is not null) THEN
                    'ENTEFFDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:ENTEFFDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ENTEXPDATE), '1900-01-01')) is null and 
                    src:ENTEXPDATE is not null) THEN
                    'ENTEXPDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:ENTEXPDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ENTORD), '0'), 38, 10) is null and 
                    src:ENTORD is not null) THEN
                    'ENTORD contains a non-numeric value : \'' || AS_VARCHAR(src:ENTORD) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ENTPCT), '0'), 38, 10) is null and 
                    src:ENTPCT is not null) THEN
                    'ENTPCT contains a non-numeric value : \'' || AS_VARCHAR(src:ENTPCT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ENTVAL1), '0'), 38, 10) is null and 
                    src:ENTVAL1 is not null) THEN
                    'ENTVAL1 contains a non-numeric value : \'' || AS_VARCHAR(src:ENTVAL1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ENTVAL2), '0'), 38, 10) is null and 
                    src:ENTVAL2 is not null) THEN
                    'ENTVAL2 contains a non-numeric value : \'' || AS_VARCHAR(src:ENTVAL2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ENTVAL3), '0'), 38, 10) is null and 
                    src:ENTVAL3 is not null) THEN
                    'ENTVAL3 contains a non-numeric value : \'' || AS_VARCHAR(src:ENTVAL3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ENTVAL4), '0'), 38, 10) is null and 
                    src:ENTVAL4 is not null) THEN
                    'ENTVAL4 contains a non-numeric value : \'' || AS_VARCHAR(src:ENTVAL4) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ENTVAL5), '0'), 38, 10) is null and 
                    src:ENTVAL5 is not null) THEN
                    'ENTVAL5 contains a non-numeric value : \'' || AS_VARCHAR(src:ENTVAL5) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ENTVAL6), '0'), 38, 10) is null and 
                    src:ENTVAL6 is not null) THEN
                    'ENTVAL6 contains a non-numeric value : \'' || AS_VARCHAR(src:ENTVAL6) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ENTVAL7), '0'), 38, 10) is null and 
                    src:ENTVAL7 is not null) THEN
                    'ENTVAL7 contains a non-numeric value : \'' || AS_VARCHAR(src:ENTVAL7) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ENTVAL8), '0'), 38, 10) is null and 
                    src:ENTVAL8 is not null) THEN
                    'ENTVAL8 contains a non-numeric value : \'' || AS_VARCHAR(src:ENTVAL8) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:IDKEY), '0'), 38, 10) is null and 
                    src:IDKEY is not null) THEN
                    'IDKEY contains a non-numeric value : \'' || AS_VARCHAR(src:IDKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
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
                                    
                src:ACCOUNTCONTACTKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_ACCTCNTC as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTCONTACTKEY), '0'), 38, 10) is null and 
                    src:ACCOUNTCONTACTKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTKEY), '0'), 38, 10) is null and 
                    src:ACCOUNTKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CAPFRDTTM), '1900-01-01')) is null and 
                    src:CAPFRDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CAPTODTTM), '1900-01-01')) is null and 
                    src:CAPTODTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CNTCTKEY), '0'), 38, 10) is null and 
                    src:CNTCTKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ENTEFFDATE), '1900-01-01')) is null and 
                    src:ENTEFFDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ENTEXPDATE), '1900-01-01')) is null and 
                    src:ENTEXPDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ENTORD), '0'), 38, 10) is null and 
                    src:ENTORD is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ENTPCT), '0'), 38, 10) is null and 
                    src:ENTPCT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ENTVAL1), '0'), 38, 10) is null and 
                    src:ENTVAL1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ENTVAL2), '0'), 38, 10) is null and 
                    src:ENTVAL2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ENTVAL3), '0'), 38, 10) is null and 
                    src:ENTVAL3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ENTVAL4), '0'), 38, 10) is null and 
                    src:ENTVAL4 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ENTVAL5), '0'), 38, 10) is null and 
                    src:ENTVAL5 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ENTVAL6), '0'), 38, 10) is null and 
                    src:ENTVAL6 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ENTVAL7), '0'), 38, 10) is null and 
                    src:ENTVAL7 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ENTVAL8), '0'), 38, 10) is null and 
                    src:ENTVAL8 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:IDKEY), '0'), 38, 10) is null and 
                    src:IDKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)