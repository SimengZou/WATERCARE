CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_BILLING_REFUNDPAYMENT_ERROR AS SELECT src, 'IPS_BILLING_REFUNDPAYMENT' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BANKINGKEY), '0'), 38, 10) is null and 
                    src:BANKINGKEY is not null) THEN
                    'BANKINGKEY contains a non-numeric value : \'' || AS_VARCHAR(src:BANKINGKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CONTACTKEY), '0'), 38, 10) is null and 
                    src:CONTACTKEY is not null) THEN
                    'CONTACTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:CONTACTKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ESCROWACCOUNTKEY), '0'), 38, 10) is null and 
                    src:ESCROWACCOUNTKEY is not null) THEN
                    'ESCROWACCOUNTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ESCROWACCOUNTKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYAMOUNT), '0'), 38, 10) is null and 
                    src:PAYAMOUNT is not null) THEN
                    'PAYAMOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:PAYAMOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:REFUNDCHECKISSUEDDTTM), '1900-01-01')) is null and 
                    src:REFUNDCHECKISSUEDDTTM is not null) THEN
                    'REFUNDCHECKISSUEDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:REFUNDCHECKISSUEDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REFUNDKEY), '0'), 38, 10) is null and 
                    src:REFUNDKEY is not null) THEN
                    'REFUNDKEY contains a non-numeric value : \'' || AS_VARCHAR(src:REFUNDKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REFUNDPAYMENTKEY), '0'), 38, 10) is null and 
                    src:REFUNDPAYMENTKEY is not null) THEN
                    'REFUNDPAYMENTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:REFUNDPAYMENTKEY) || '\'' WHEN 
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
                                    
                src:REFUNDPAYMENTKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_REFUNDPAYMENT as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BANKINGKEY), '0'), 38, 10) is null and 
                    src:BANKINGKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CONTACTKEY), '0'), 38, 10) is null and 
                    src:CONTACTKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ESCROWACCOUNTKEY), '0'), 38, 10) is null and 
                    src:ESCROWACCOUNTKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYAMOUNT), '0'), 38, 10) is null and 
                    src:PAYAMOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:REFUNDCHECKISSUEDDTTM), '1900-01-01')) is null and 
                    src:REFUNDCHECKISSUEDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REFUNDKEY), '0'), 38, 10) is null and 
                    src:REFUNDKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REFUNDPAYMENTKEY), '0'), 38, 10) is null and 
                    src:REFUNDPAYMENTKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)