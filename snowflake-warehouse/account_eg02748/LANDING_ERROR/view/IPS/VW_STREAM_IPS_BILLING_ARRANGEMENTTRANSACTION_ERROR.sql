CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_BILLING_ARRANGEMENTTRANSACTION_ERROR AS SELECT src, 'IPS_BILLING_ARRANGEMENTTRANSACTION' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ARRANGEMENTTRANSACTIONKEY), '0'), 38, 10) is null and 
                    src:ARRANGEMENTTRANSACTIONKEY is not null) THEN
                    'ARRANGEMENTTRANSACTIONKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ARRANGEMENTTRANSACTIONKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLKEY), '0'), 38, 10) is null and 
                    src:BILLKEY is not null) THEN
                    'BILLKEY contains a non-numeric value : \'' || AS_VARCHAR(src:BILLKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYMENTARRANGEMENTKEY), '0'), 38, 10) is null and 
                    src:PAYMENTARRANGEMENTKEY is not null) THEN
                    'PAYMENTARRANGEMENTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:PAYMENTARRANGEMENTKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYMENTKEY), '0'), 38, 10) is null and 
                    src:PAYMENTKEY is not null) THEN
                    'PAYMENTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:PAYMENTKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRANSACTIONAMT), '0'), 38, 10) is null and 
                    src:TRANSACTIONAMT is not null) THEN
                    'TRANSACTIONAMT contains a non-numeric value : \'' || AS_VARCHAR(src:TRANSACTIONAMT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRANSACTIONDTTM), '1900-01-01')) is null and 
                    src:TRANSACTIONDTTM is not null) THEN
                    'TRANSACTIONDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:TRANSACTIONDTTM) || '\'' WHEN 
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
                                    
                src:ARRANGEMENTTRANSACTIONKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_ARRANGEMENTTRANSACTION as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ARRANGEMENTTRANSACTIONKEY), '0'), 38, 10) is null and 
                    src:ARRANGEMENTTRANSACTIONKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLKEY), '0'), 38, 10) is null and 
                    src:BILLKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYMENTARRANGEMENTKEY), '0'), 38, 10) is null and 
                    src:PAYMENTARRANGEMENTKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYMENTKEY), '0'), 38, 10) is null and 
                    src:PAYMENTKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRANSACTIONAMT), '0'), 38, 10) is null and 
                    src:TRANSACTIONAMT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRANSACTIONDTTM), '1900-01-01')) is null and 
                    src:TRANSACTIONDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)