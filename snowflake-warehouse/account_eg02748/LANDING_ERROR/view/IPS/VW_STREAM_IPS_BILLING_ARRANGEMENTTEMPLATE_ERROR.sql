CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_BILLING_ARRANGEMENTTEMPLATE_ERROR AS SELECT src, 'IPS_BILLING_ARRANGEMENTTEMPLATE' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ARRANGEDPAYMENTAMOUNT), '0'), 38, 10) is null and 
                    src:ARRANGEDPAYMENTAMOUNT is not null) THEN
                    'ARRANGEDPAYMENTAMOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:ARRANGEDPAYMENTAMOUNT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ARRANGEMENTTEMPLATEKEY), '0'), 38, 10) is null and 
                    src:ARRANGEMENTTEMPLATEKEY is not null) THEN
                    'ARRANGEMENTTEMPLATEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ARRANGEMENTTEMPLATEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DOWNPAYMENTAMT), '0'), 38, 10) is null and 
                    src:DOWNPAYMENTAMT is not null) THEN
                    'DOWNPAYMENTAMT contains a non-numeric value : \'' || AS_VARCHAR(src:DOWNPAYMENTAMT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DOWNPAYMENTDUEDAYS), '0'), 38, 10) is null and 
                    src:DOWNPAYMENTDUEDAYS is not null) THEN
                    'DOWNPAYMENTDUEDAYS contains a non-numeric value : \'' || AS_VARCHAR(src:DOWNPAYMENTDUEDAYS) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMBEROFPAYMENTS), '0'), 38, 10) is null and 
                    src:NUMBEROFPAYMENTS is not null) THEN
                    'NUMBEROFPAYMENTS contains a non-numeric value : \'' || AS_VARCHAR(src:NUMBEROFPAYMENTS) || '\'' WHEN 
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
                                    
                src:ARRANGEMENTTEMPLATEKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_ARRANGEMENTTEMPLATE as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ARRANGEDPAYMENTAMOUNT), '0'), 38, 10) is null and 
                    src:ARRANGEDPAYMENTAMOUNT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ARRANGEMENTTEMPLATEKEY), '0'), 38, 10) is null and 
                    src:ARRANGEMENTTEMPLATEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DOWNPAYMENTAMT), '0'), 38, 10) is null and 
                    src:DOWNPAYMENTAMT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DOWNPAYMENTDUEDAYS), '0'), 38, 10) is null and 
                    src:DOWNPAYMENTDUEDAYS is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMBEROFPAYMENTS), '0'), 38, 10) is null and 
                    src:NUMBEROFPAYMENTS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)