CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_BILLING_ACCOUNTTRADEWASTE_ERROR AS SELECT src, 'IPS_BILLING_ACCOUNTTRADEWASTE' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTKEY), '0'), 38, 10) is null and 
                    src:ACCOUNTKEY is not null) THEN
                    'ACCOUNTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ACCOUNTKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTSERVICEKEY), '0'), 38, 10) is null and 
                    src:ACCOUNTSERVICEKEY is not null) THEN
                    'ACCOUNTSERVICEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ACCOUNTSERVICEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTTRADEWASTEKEY), '0'), 38, 10) is null and 
                    src:ACCOUNTTRADEWASTEKEY is not null) THEN
                    'ACCOUNTTRADEWASTEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ACCOUNTTRADEWASTEKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CNTCTKEY), '0'), 38, 10) is null and 
                    src:CNTCTKEY is not null) THEN
                    'CNTCTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:CNTCTKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMMENTKEY), '0'), 38, 10) is null and 
                    src:COMMENTKEY is not null) THEN
                    'COMMENTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:COMMENTKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:GROSS), '0'), 38, 10) is null and 
                    src:GROSS is not null) THEN
                    'GROSS contains a non-numeric value : \'' || AS_VARCHAR(src:GROSS) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:INDTTM), '1900-01-01')) is null and 
                    src:INDTTM is not null) THEN
                    'INDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:INDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LATEFEEAMOUNT), '0'), 38, 10) is null and 
                    src:LATEFEEAMOUNT is not null) THEN
                    'LATEFEEAMOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:LATEFEEAMOUNT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LINEITEMKEY), '0'), 38, 10) is null and 
                    src:LINEITEMKEY is not null) THEN
                    'LINEITEMKEY contains a non-numeric value : \'' || AS_VARCHAR(src:LINEITEMKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NET), '0'), 38, 10) is null and 
                    src:NET is not null) THEN
                    'NET contains a non-numeric value : \'' || AS_VARCHAR(src:NET) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NOMINATEDONEOFF), '0'), 38, 10) is null and 
                    src:NOMINATEDONEOFF is not null) THEN
                    'NOMINATEDONEOFF contains a non-numeric value : \'' || AS_VARCHAR(src:NOMINATEDONEOFF) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OUTDTTM), '1900-01-01')) is null and 
                    src:OUTDTTM is not null) THEN
                    'OUTDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:OUTDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TARE), '0'), 38, 10) is null and 
                    src:TARE is not null) THEN
                    'TARE contains a non-numeric value : \'' || AS_VARCHAR(src:TARE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRAILERREGKEY), '0'), 38, 10) is null and 
                    src:TRAILERREGKEY is not null) THEN
                    'TRAILERREGKEY contains a non-numeric value : \'' || AS_VARCHAR(src:TRAILERREGKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TWVEHICLEREGKEY), '0'), 38, 10) is null and 
                    src:TWVEHICLEREGKEY is not null) THEN
                    'TWVEHICLEREGKEY contains a non-numeric value : \'' || AS_VARCHAR(src:TWVEHICLEREGKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:UNIT), '0'), 38, 10) is null and 
                    src:UNIT is not null) THEN
                    'UNIT contains a non-numeric value : \'' || AS_VARCHAR(src:UNIT) || '\'' WHEN 
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
                                    
                src:ACCOUNTTRADEWASTEKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_ACCOUNTTRADEWASTE as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTKEY), '0'), 38, 10) is null and 
                    src:ACCOUNTKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTSERVICEKEY), '0'), 38, 10) is null and 
                    src:ACCOUNTSERVICEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTTRADEWASTEKEY), '0'), 38, 10) is null and 
                    src:ACCOUNTTRADEWASTEKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CNTCTKEY), '0'), 38, 10) is null and 
                    src:CNTCTKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMMENTKEY), '0'), 38, 10) is null and 
                    src:COMMENTKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:GROSS), '0'), 38, 10) is null and 
                    src:GROSS is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:INDTTM), '1900-01-01')) is null and 
                    src:INDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LATEFEEAMOUNT), '0'), 38, 10) is null and 
                    src:LATEFEEAMOUNT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LINEITEMKEY), '0'), 38, 10) is null and 
                    src:LINEITEMKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NET), '0'), 38, 10) is null and 
                    src:NET is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NOMINATEDONEOFF), '0'), 38, 10) is null and 
                    src:NOMINATEDONEOFF is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OUTDTTM), '1900-01-01')) is null and 
                    src:OUTDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TARE), '0'), 38, 10) is null and 
                    src:TARE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRAILERREGKEY), '0'), 38, 10) is null and 
                    src:TRAILERREGKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TWVEHICLEREGKEY), '0'), 38, 10) is null and 
                    src:TWVEHICLEREGKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:UNIT), '0'), 38, 10) is null and 
                    src:UNIT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)