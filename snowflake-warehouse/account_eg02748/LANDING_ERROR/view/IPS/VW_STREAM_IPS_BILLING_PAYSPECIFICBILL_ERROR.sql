CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_BILLING_PAYSPECIFICBILL_ERROR AS SELECT src, 'IPS_BILLING_PAYSPECIFICBILL' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADJUSTMENTKEY), '0'), 38, 10) is null and 
                    src:ADJUSTMENTKEY is not null) THEN
                    'ADJUSTMENTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ADJUSTMENTKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLKEY), '0'), 38, 10) is null and 
                    src:BILLKEY is not null) THEN
                    'BILLKEY contains a non-numeric value : \'' || AS_VARCHAR(src:BILLKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CDRAPPLICATIONFEEKEY), '0'), 38, 10) is null and 
                    src:CDRAPPLICATIONFEEKEY is not null) THEN
                    'CDRAPPLICATIONFEEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:CDRAPPLICATIONFEEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DEPOSITADJUSTMENTKEY), '0'), 38, 10) is null and 
                    src:DEPOSITADJUSTMENTKEY is not null) THEN
                    'DEPOSITADJUSTMENTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:DEPOSITADJUSTMENTKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DEPOSITCHARGEKEY), '0'), 38, 10) is null and 
                    src:DEPOSITCHARGEKEY is not null) THEN
                    'DEPOSITCHARGEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:DEPOSITCHARGEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LINEITEMKEY), '0'), 38, 10) is null and 
                    src:LINEITEMKEY is not null) THEN
                    'LINEITEMKEY contains a non-numeric value : \'' || AS_VARCHAR(src:LINEITEMKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ONEOFFCHARGEKEY), '0'), 38, 10) is null and 
                    src:ONEOFFCHARGEKEY is not null) THEN
                    'ONEOFFCHARGEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ONEOFFCHARGEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORIGPAYALLOCAMT), '0'), 38, 10) is null and 
                    src:ORIGPAYALLOCAMT is not null) THEN
                    'ORIGPAYALLOCAMT contains a non-numeric value : \'' || AS_VARCHAR(src:ORIGPAYALLOCAMT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYMENTKEY), '0'), 38, 10) is null and 
                    src:PAYMENTKEY is not null) THEN
                    'PAYMENTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:PAYMENTKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYORDER), '0'), 38, 10) is null and 
                    src:PAYORDER is not null) THEN
                    'PAYORDER contains a non-numeric value : \'' || AS_VARCHAR(src:PAYORDER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYSPECIFICBILLKEY), '0'), 38, 10) is null and 
                    src:PAYSPECIFICBILLKEY is not null) THEN
                    'PAYSPECIFICBILLKEY contains a non-numeric value : \'' || AS_VARCHAR(src:PAYSPECIFICBILLKEY) || '\'' WHEN 
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
                                    
                src:PAYSPECIFICBILLKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_PAYSPECIFICBILL as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADJUSTMENTKEY), '0'), 38, 10) is null and 
                    src:ADJUSTMENTKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLKEY), '0'), 38, 10) is null and 
                    src:BILLKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CDRAPPLICATIONFEEKEY), '0'), 38, 10) is null and 
                    src:CDRAPPLICATIONFEEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DEPOSITADJUSTMENTKEY), '0'), 38, 10) is null and 
                    src:DEPOSITADJUSTMENTKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DEPOSITCHARGEKEY), '0'), 38, 10) is null and 
                    src:DEPOSITCHARGEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LINEITEMKEY), '0'), 38, 10) is null and 
                    src:LINEITEMKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ONEOFFCHARGEKEY), '0'), 38, 10) is null and 
                    src:ONEOFFCHARGEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORIGPAYALLOCAMT), '0'), 38, 10) is null and 
                    src:ORIGPAYALLOCAMT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYMENTKEY), '0'), 38, 10) is null and 
                    src:PAYMENTKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYORDER), '0'), 38, 10) is null and 
                    src:PAYORDER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYSPECIFICBILLKEY), '0'), 38, 10) is null and 
                    src:PAYSPECIFICBILLKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)