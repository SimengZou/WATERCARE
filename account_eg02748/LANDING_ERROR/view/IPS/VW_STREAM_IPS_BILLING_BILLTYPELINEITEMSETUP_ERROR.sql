CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_BILLING_BILLTYPELINEITEMSETUP_ERROR AS SELECT src, 'IPS_BILLING_BILLTYPELINEITEMSETUP' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLTYPEKEY), '0'), 38, 10) is null and 
                    src:BILLTYPEKEY is not null) THEN
                    'BILLTYPEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:BILLTYPEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLTYPELINEITEMSETUPKEY), '0'), 38, 10) is null and 
                    src:BILLTYPELINEITEMSETUPKEY is not null) THEN
                    'BILLTYPELINEITEMSETUPKEY contains a non-numeric value : \'' || AS_VARCHAR(src:BILLTYPELINEITEMSETUPKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CALCULATIONORDER), '0'), 38, 10) is null and 
                    src:CALCULATIONORDER is not null) THEN
                    'CALCULATIONORDER contains a non-numeric value : \'' || AS_VARCHAR(src:CALCULATIONORDER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LINEITEMSETUPKEY), '0'), 38, 10) is null and 
                    src:LINEITEMSETUPKEY is not null) THEN
                    'LINEITEMSETUPKEY contains a non-numeric value : \'' || AS_VARCHAR(src:LINEITEMSETUPKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
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
                                    
                src:BILLTYPELINEITEMSETUPKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_BILLTYPELINEITEMSETUP as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLTYPEKEY), '0'), 38, 10) is null and 
                    src:BILLTYPEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLTYPELINEITEMSETUPKEY), '0'), 38, 10) is null and 
                    src:BILLTYPELINEITEMSETUPKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CALCULATIONORDER), '0'), 38, 10) is null and 
                    src:CALCULATIONORDER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LINEITEMSETUPKEY), '0'), 38, 10) is null and 
                    src:LINEITEMSETUPKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
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