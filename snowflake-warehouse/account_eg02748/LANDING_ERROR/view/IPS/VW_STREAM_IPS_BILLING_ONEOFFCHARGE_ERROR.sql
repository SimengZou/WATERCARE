CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_BILLING_ONEOFFCHARGE_ERROR AS SELECT src, 'IPS_BILLING_ONEOFFCHARGE' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTKEY), '0'), 38, 10) is null and 
                    src:ACCOUNTKEY is not null) THEN
                    'ACCOUNTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ACCOUNTKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AMOUNT), '0'), 38, 10) is null and 
                    src:AMOUNT is not null) THEN
                    'AMOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:AMOUNT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLTYPEKEY), '0'), 38, 10) is null and 
                    src:BILLTYPEKEY is not null) THEN
                    'BILLTYPEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:BILLTYPEKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CHARGEDDTTM), '1900-01-01')) is null and 
                    src:CHARGEDDTTM is not null) THEN
                    'CHARGEDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:CHARGEDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMMENTSKEY), '0'), 38, 10) is null and 
                    src:COMMENTSKEY is not null) THEN
                    'COMMENTSKEY contains a non-numeric value : \'' || AS_VARCHAR(src:COMMENTSKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LINEITEMSETUPKEY), '0'), 38, 10) is null and 
                    src:LINEITEMSETUPKEY is not null) THEN
                    'LINEITEMSETUPKEY contains a non-numeric value : \'' || AS_VARCHAR(src:LINEITEMSETUPKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ONEOFFCHARGEKEY), '0'), 38, 10) is null and 
                    src:ONEOFFCHARGEKEY is not null) THEN
                    'ONEOFFCHARGEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ONEOFFCHARGEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYMENTKEY), '0'), 38, 10) is null and 
                    src:PAYMENTKEY is not null) THEN
                    'PAYMENTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:PAYMENTKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:QUANTITY), '0'), 38, 10) is null and 
                    src:QUANTITY is not null) THEN
                    'QUANTITY contains a non-numeric value : \'' || AS_VARCHAR(src:QUANTITY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SERVICEREQUESTNUMBER), '0'), 38, 10) is null and 
                    src:SERVICEREQUESTNUMBER is not null) THEN
                    'SERVICEREQUESTNUMBER contains a non-numeric value : \'' || AS_VARCHAR(src:SERVICEREQUESTNUMBER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SUBTOTAL), '0'), 38, 10) is null and 
                    src:SUBTOTAL is not null) THEN
                    'SUBTOTAL contains a non-numeric value : \'' || AS_VARCHAR(src:SUBTOTAL) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VALUE), '0'), 38, 10) is null and 
                    src:VALUE is not null) THEN
                    'VALUE contains a non-numeric value : \'' || AS_VARCHAR(src:VALUE) || '\'' WHEN 
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
                                    
                src:ONEOFFCHARGEKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_ONEOFFCHARGE as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTKEY), '0'), 38, 10) is null and 
                    src:ACCOUNTKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AMOUNT), '0'), 38, 10) is null and 
                    src:AMOUNT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLTYPEKEY), '0'), 38, 10) is null and 
                    src:BILLTYPEKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CHARGEDDTTM), '1900-01-01')) is null and 
                    src:CHARGEDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMMENTSKEY), '0'), 38, 10) is null and 
                    src:COMMENTSKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LINEITEMSETUPKEY), '0'), 38, 10) is null and 
                    src:LINEITEMSETUPKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ONEOFFCHARGEKEY), '0'), 38, 10) is null and 
                    src:ONEOFFCHARGEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYMENTKEY), '0'), 38, 10) is null and 
                    src:PAYMENTKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:QUANTITY), '0'), 38, 10) is null and 
                    src:QUANTITY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SERVICEREQUESTNUMBER), '0'), 38, 10) is null and 
                    src:SERVICEREQUESTNUMBER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SUBTOTAL), '0'), 38, 10) is null and 
                    src:SUBTOTAL is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VALUE), '0'), 38, 10) is null and 
                    src:VALUE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)