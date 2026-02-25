CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_BILLING_NSFIMPORTACTIVITYDETAILS_ERROR AS SELECT src, 'IPS_BILLING_NSFIMPORTACTIVITYDETAILS' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AMOUNT), '0'), 38, 10) is null and 
                    src:AMOUNT is not null) THEN
                    'AMOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:AMOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NSFIMPORTACTIVITYDTLKEY), '0'), 38, 10) is null and 
                    src:NSFIMPORTACTIVITYDTLKEY is not null) THEN
                    'NSFIMPORTACTIVITYDTLKEY contains a non-numeric value : \'' || AS_VARCHAR(src:NSFIMPORTACTIVITYDTLKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NSFIMPORTACTIVITYKEY), '0'), 38, 10) is null and 
                    src:NSFIMPORTACTIVITYKEY is not null) THEN
                    'NSFIMPORTACTIVITYKEY contains a non-numeric value : \'' || AS_VARCHAR(src:NSFIMPORTACTIVITYKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NSFPROCESSEXCEPTION), '0'), 38, 10) is null and 
                    src:NSFPROCESSEXCEPTION is not null) THEN
                    'NSFPROCESSEXCEPTION contains a non-numeric value : \'' || AS_VARCHAR(src:NSFPROCESSEXCEPTION) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYMENTBATCHKEY), '0'), 38, 10) is null and 
                    src:PAYMENTBATCHKEY is not null) THEN
                    'PAYMENTBATCHKEY contains a non-numeric value : \'' || AS_VARCHAR(src:PAYMENTBATCHKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYMENTIDENTIFIER), '0'), 38, 10) is null and 
                    src:PAYMENTIDENTIFIER is not null) THEN
                    'PAYMENTIDENTIFIER contains a non-numeric value : \'' || AS_VARCHAR(src:PAYMENTIDENTIFIER) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:REVIEWDATE), '1900-01-01')) is null and 
                    src:REVIEWDATE is not null) THEN
                    'REVIEWDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:REVIEWDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRANSACTIONDATE), '1900-01-01')) is null and 
                    src:TRANSACTIONDATE is not null) THEN
                    'TRANSACTIONDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:TRANSACTIONDATE) || '\'' WHEN 
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
                                    
                src:NSFIMPORTACTIVITYDTLKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_NSFIMPORTACTIVITYDETAILS as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AMOUNT), '0'), 38, 10) is null and 
                    src:AMOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NSFIMPORTACTIVITYDTLKEY), '0'), 38, 10) is null and 
                    src:NSFIMPORTACTIVITYDTLKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NSFIMPORTACTIVITYKEY), '0'), 38, 10) is null and 
                    src:NSFIMPORTACTIVITYKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NSFPROCESSEXCEPTION), '0'), 38, 10) is null and 
                    src:NSFPROCESSEXCEPTION is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYMENTBATCHKEY), '0'), 38, 10) is null and 
                    src:PAYMENTBATCHKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYMENTIDENTIFIER), '0'), 38, 10) is null and 
                    src:PAYMENTIDENTIFIER is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:REVIEWDATE), '1900-01-01')) is null and 
                    src:REVIEWDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRANSACTIONDATE), '1900-01-01')) is null and 
                    src:TRANSACTIONDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)