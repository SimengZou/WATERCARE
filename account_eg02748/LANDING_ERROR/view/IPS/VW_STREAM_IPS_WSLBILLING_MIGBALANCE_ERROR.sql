CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_WSLBILLING_MIGBALANCE_ERROR AS SELECT src, 'IPS_WSLBILLING_MIGBALANCE' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLKEY), '0'), 38, 10) is null and 
                    src:BILLKEY is not null) THEN
                    'BILLKEY contains a non-numeric value : \'' || AS_VARCHAR(src:BILLKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CREDITBALANCE), '0'), 38, 10) is null and 
                    src:CREDITBALANCE is not null) THEN
                    'CREDITBALANCE contains a non-numeric value : \'' || AS_VARCHAR(src:CREDITBALANCE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DEBITBALANCE), '0'), 38, 10) is null and 
                    src:DEBITBALANCE is not null) THEN
                    'DEBITBALANCE contains a non-numeric value : \'' || AS_VARCHAR(src:DEBITBALANCE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MIGBALANCEKEY), '0'), 38, 10) is null and 
                    src:MIGBALANCEKEY is not null) THEN
                    'MIGBALANCEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:MIGBALANCEKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PENALTY), '0'), 38, 10) is null and 
                    src:PENALTY is not null) THEN
                    'PENALTY contains a non-numeric value : \'' || AS_VARCHAR(src:PENALTY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) THEN
                    'VARIATION_ID contains a non-numeric value : \'' || AS_VARCHAR(src:VARIATION_ID) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WASTEBALANCE), '0'), 38, 10) is null and 
                    src:WASTEBALANCE is not null) THEN
                    'WASTEBALANCE contains a non-numeric value : \'' || AS_VARCHAR(src:WASTEBALANCE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WASTEBALANCEF), '0'), 38, 10) is null and 
                    src:WASTEBALANCEF is not null) THEN
                    'WASTEBALANCEF contains a non-numeric value : \'' || AS_VARCHAR(src:WASTEBALANCEF) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WATERBALANCE), '0'), 38, 10) is null and 
                    src:WATERBALANCE is not null) THEN
                    'WATERBALANCE contains a non-numeric value : \'' || AS_VARCHAR(src:WATERBALANCE) || '\'' WHEN 
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
                                    
                src:MIGBALANCEKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_WSLBILLING_MIGBALANCE as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLKEY), '0'), 38, 10) is null and 
                    src:BILLKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CREDITBALANCE), '0'), 38, 10) is null and 
                    src:CREDITBALANCE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DEBITBALANCE), '0'), 38, 10) is null and 
                    src:DEBITBALANCE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MIGBALANCEKEY), '0'), 38, 10) is null and 
                    src:MIGBALANCEKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PENALTY), '0'), 38, 10) is null and 
                    src:PENALTY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WASTEBALANCE), '0'), 38, 10) is null and 
                    src:WASTEBALANCE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WASTEBALANCEF), '0'), 38, 10) is null and 
                    src:WASTEBALANCEF is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WATERBALANCE), '0'), 38, 10) is null and 
                    src:WATERBALANCE is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)