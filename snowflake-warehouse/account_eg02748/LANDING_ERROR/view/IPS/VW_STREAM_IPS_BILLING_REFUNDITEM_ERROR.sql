CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_BILLING_REFUNDITEM_ERROR AS SELECT src, 'IPS_BILLING_REFUNDITEM' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADJUSTMENTKEY), '0'), 38, 10) is null and 
                    src:ADJUSTMENTKEY is not null) THEN
                    'ADJUSTMENTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ADJUSTMENTKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DEPOSITADJUSTMENT), '0'), 38, 10) is null and 
                    src:DEPOSITADJUSTMENT is not null) THEN
                    'DEPOSITADJUSTMENT contains a non-numeric value : \'' || AS_VARCHAR(src:DEPOSITADJUSTMENT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DEPOSITCHARGEKEY), '0'), 38, 10) is null and 
                    src:DEPOSITCHARGEKEY is not null) THEN
                    'DEPOSITCHARGEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:DEPOSITCHARGEKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYKEY), '0'), 38, 10) is null and 
                    src:PAYKEY is not null) THEN
                    'PAYKEY contains a non-numeric value : \'' || AS_VARCHAR(src:PAYKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REFUNDITEMAMOUNT), '0'), 38, 10) is null and 
                    src:REFUNDITEMAMOUNT is not null) THEN
                    'REFUNDITEMAMOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:REFUNDITEMAMOUNT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REFUNDITEMKEY), '0'), 38, 10) is null and 
                    src:REFUNDITEMKEY is not null) THEN
                    'REFUNDITEMKEY contains a non-numeric value : \'' || AS_VARCHAR(src:REFUNDITEMKEY) || '\'' WHEN 
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
                                    
                src:REFUNDITEMKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_REFUNDITEM as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADJUSTMENTKEY), '0'), 38, 10) is null and 
                    src:ADJUSTMENTKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DEPOSITADJUSTMENT), '0'), 38, 10) is null and 
                    src:DEPOSITADJUSTMENT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DEPOSITCHARGEKEY), '0'), 38, 10) is null and 
                    src:DEPOSITCHARGEKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYKEY), '0'), 38, 10) is null and 
                    src:PAYKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REFUNDITEMAMOUNT), '0'), 38, 10) is null and 
                    src:REFUNDITEMAMOUNT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REFUNDITEMKEY), '0'), 38, 10) is null and 
                    src:REFUNDITEMKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REFUNDKEY), '0'), 38, 10) is null and 
                    src:REFUNDKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REFUNDPAYMENTKEY), '0'), 38, 10) is null and 
                    src:REFUNDPAYMENTKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)