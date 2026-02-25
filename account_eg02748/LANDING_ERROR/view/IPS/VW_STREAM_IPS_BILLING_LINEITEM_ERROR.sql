CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_BILLING_LINEITEM_ERROR AS SELECT src, 'IPS_BILLING_LINEITEM' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTSERVICEKEY), '0'), 38, 10) is null and 
                    src:ACCOUNTSERVICEKEY is not null) THEN
                    'ACCOUNTSERVICEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ACCOUNTSERVICEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACTUALAMT), '0'), 38, 10) is null and 
                    src:ACTUALAMT is not null) THEN
                    'ACTUALAMT contains a non-numeric value : \'' || AS_VARCHAR(src:ACTUALAMT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLKEY), '0'), 38, 10) is null and 
                    src:BILLKEY is not null) THEN
                    'BILLKEY contains a non-numeric value : \'' || AS_VARCHAR(src:BILLKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMMENTSKEY), '0'), 38, 10) is null and 
                    src:COMMENTSKEY is not null) THEN
                    'COMMENTSKEY contains a non-numeric value : \'' || AS_VARCHAR(src:COMMENTSKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DISCOUNTEXPIREDATE), '1900-01-01')) is null and 
                    src:DISCOUNTEXPIREDATE is not null) THEN
                    'DISCOUNTEXPIREDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:DISCOUNTEXPIREDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LINEITEMAMT), '0'), 38, 10) is null and 
                    src:LINEITEMAMT is not null) THEN
                    'LINEITEMAMT contains a non-numeric value : \'' || AS_VARCHAR(src:LINEITEMAMT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LINEITEMKEY), '0'), 38, 10) is null and 
                    src:LINEITEMKEY is not null) THEN
                    'LINEITEMKEY contains a non-numeric value : \'' || AS_VARCHAR(src:LINEITEMKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LINEITEMSETUPKEY), '0'), 38, 10) is null and 
                    src:LINEITEMSETUPKEY is not null) THEN
                    'LINEITEMSETUPKEY contains a non-numeric value : \'' || AS_VARCHAR(src:LINEITEMSETUPKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LINEITEMUNITS), '0'), 38, 10) is null and 
                    src:LINEITEMUNITS is not null) THEN
                    'LINEITEMUNITS contains a non-numeric value : \'' || AS_VARCHAR(src:LINEITEMUNITS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LINEITEMUSAGE), '0'), 38, 10) is null and 
                    src:LINEITEMUSAGE is not null) THEN
                    'LINEITEMUSAGE contains a non-numeric value : \'' || AS_VARCHAR(src:LINEITEMUSAGE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PENALTYDATE), '1900-01-01')) is null and 
                    src:PENALTYDATE is not null) THEN
                    'PENALTYDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:PENALTYDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PENALTYPAYORDER), '0'), 38, 10) is null and 
                    src:PENALTYPAYORDER is not null) THEN
                    'PENALTYPAYORDER contains a non-numeric value : \'' || AS_VARCHAR(src:PENALTYPAYORDER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PRINCIPALPAYORDER), '0'), 38, 10) is null and 
                    src:PRINCIPALPAYORDER is not null) THEN
                    'PRINCIPALPAYORDER contains a non-numeric value : \'' || AS_VARCHAR(src:PRINCIPALPAYORDER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PRINTORDER), '0'), 38, 10) is null and 
                    src:PRINTORDER is not null) THEN
                    'PRINTORDER contains a non-numeric value : \'' || AS_VARCHAR(src:PRINTORDER) || '\'' WHEN 
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
                                    
                src:LINEITEMKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_LINEITEM as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTSERVICEKEY), '0'), 38, 10) is null and 
                    src:ACCOUNTSERVICEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACTUALAMT), '0'), 38, 10) is null and 
                    src:ACTUALAMT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLKEY), '0'), 38, 10) is null and 
                    src:BILLKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMMENTSKEY), '0'), 38, 10) is null and 
                    src:COMMENTSKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DISCOUNTEXPIREDATE), '1900-01-01')) is null and 
                    src:DISCOUNTEXPIREDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LINEITEMAMT), '0'), 38, 10) is null and 
                    src:LINEITEMAMT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LINEITEMKEY), '0'), 38, 10) is null and 
                    src:LINEITEMKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LINEITEMSETUPKEY), '0'), 38, 10) is null and 
                    src:LINEITEMSETUPKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LINEITEMUNITS), '0'), 38, 10) is null and 
                    src:LINEITEMUNITS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LINEITEMUSAGE), '0'), 38, 10) is null and 
                    src:LINEITEMUSAGE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PENALTYDATE), '1900-01-01')) is null and 
                    src:PENALTYDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PENALTYPAYORDER), '0'), 38, 10) is null and 
                    src:PENALTYPAYORDER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PRINCIPALPAYORDER), '0'), 38, 10) is null and 
                    src:PRINCIPALPAYORDER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PRINTORDER), '0'), 38, 10) is null and 
                    src:PRINTORDER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)