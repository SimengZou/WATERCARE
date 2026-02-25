CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_CASHIERING_INTERNALCHARGE_ERROR AS SELECT src, 'IPS_CASHIERING_INTERNALCHARGE' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCTKEY), '0'), 38, 10) is null and 
                    src:ACCTKEY is not null) THEN
                    'ACCTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ACCTKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADJUSTMENTKEY), '0'), 38, 10) is null and 
                    src:ADJUSTMENTKEY is not null) THEN
                    'ADJUSTMENTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ADJUSTMENTKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APFEEKEY), '0'), 38, 10) is null and 
                    src:APFEEKEY is not null) THEN
                    'APFEEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:APFEEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APKEY), '0'), 38, 10) is null and 
                    src:APKEY is not null) THEN
                    'APKEY contains a non-numeric value : \'' || AS_VARCHAR(src:APKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLKEY), '0'), 38, 10) is null and 
                    src:BILLKEY is not null) THEN
                    'BILLKEY contains a non-numeric value : \'' || AS_VARCHAR(src:BILLKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DEPOSITADJKEY), '0'), 38, 10) is null and 
                    src:DEPOSITADJKEY is not null) THEN
                    'DEPOSITADJKEY contains a non-numeric value : \'' || AS_VARCHAR(src:DEPOSITADJKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DEPOSITKEY), '0'), 38, 10) is null and 
                    src:DEPOSITKEY is not null) THEN
                    'DEPOSITKEY contains a non-numeric value : \'' || AS_VARCHAR(src:DEPOSITKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ESCROWACCT), '0'), 38, 10) is null and 
                    src:ESCROWACCT is not null) THEN
                    'ESCROWACCT contains a non-numeric value : \'' || AS_VARCHAR(src:ESCROWACCT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INTERNALCHARGESKEY), '0'), 38, 10) is null and 
                    src:INTERNALCHARGESKEY is not null) THEN
                    'INTERNALCHARGESKEY contains a non-numeric value : \'' || AS_VARCHAR(src:INTERNALCHARGESKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LINEITEMKEY), '0'), 38, 10) is null and 
                    src:LINEITEMKEY is not null) THEN
                    'LINEITEMKEY contains a non-numeric value : \'' || AS_VARCHAR(src:LINEITEMKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ONEOFFKEY), '0'), 38, 10) is null and 
                    src:ONEOFFKEY is not null) THEN
                    'ONEOFFKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ONEOFFKEY) || '\'' WHEN 
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
                                    
                src:INTERNALCHARGESKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_CASHIERING_INTERNALCHARGE as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCTKEY), '0'), 38, 10) is null and 
                    src:ACCTKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADJUSTMENTKEY), '0'), 38, 10) is null and 
                    src:ADJUSTMENTKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APFEEKEY), '0'), 38, 10) is null and 
                    src:APFEEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APKEY), '0'), 38, 10) is null and 
                    src:APKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLKEY), '0'), 38, 10) is null and 
                    src:BILLKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DEPOSITADJKEY), '0'), 38, 10) is null and 
                    src:DEPOSITADJKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DEPOSITKEY), '0'), 38, 10) is null and 
                    src:DEPOSITKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ESCROWACCT), '0'), 38, 10) is null and 
                    src:ESCROWACCT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INTERNALCHARGESKEY), '0'), 38, 10) is null and 
                    src:INTERNALCHARGESKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LINEITEMKEY), '0'), 38, 10) is null and 
                    src:LINEITEMKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ONEOFFKEY), '0'), 38, 10) is null and 
                    src:ONEOFFKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)