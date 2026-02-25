CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_CDR_APBILLACCTCHNGLOG_ERROR AS SELECT src, 'IPS_CDR_APBILLACCTCHNGLOG' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCTCHANGETRANSFERTYPE), '0'), 38, 10) is null and 
                    src:ACCTCHANGETRANSFERTYPE is not null) THEN
                    'ACCTCHANGETRANSFERTYPE contains a non-numeric value : \'' || AS_VARCHAR(src:ACCTCHANGETRANSFERTYPE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APBILLACCTCHNGLOGKEY), '0'), 38, 10) is null and 
                    src:APBILLACCTCHNGLOGKEY is not null) THEN
                    'APBILLACCTCHNGLOGKEY contains a non-numeric value : \'' || AS_VARCHAR(src:APBILLACCTCHNGLOGKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APKEY), '0'), 38, 10) is null and 
                    src:APKEY is not null) THEN
                    'APKEY contains a non-numeric value : \'' || AS_VARCHAR(src:APKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FROMBILLACCTKEY), '0'), 38, 10) is null and 
                    src:FROMBILLACCTKEY is not null) THEN
                    'FROMBILLACCTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:FROMBILLACCTKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TOBILLACCTKEY), '0'), 38, 10) is null and 
                    src:TOBILLACCTKEY is not null) THEN
                    'TOBILLACCTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:TOBILLACCTKEY) || '\'' WHEN 
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
                                    
                src:APBILLACCTCHNGLOGKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_CDR_APBILLACCTCHNGLOG as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCTCHANGETRANSFERTYPE), '0'), 38, 10) is null and 
                    src:ACCTCHANGETRANSFERTYPE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APBILLACCTCHNGLOGKEY), '0'), 38, 10) is null and 
                    src:APBILLACCTCHNGLOGKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APKEY), '0'), 38, 10) is null and 
                    src:APKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FROMBILLACCTKEY), '0'), 38, 10) is null and 
                    src:FROMBILLACCTKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TOBILLACCTKEY), '0'), 38, 10) is null and 
                    src:TOBILLACCTKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)