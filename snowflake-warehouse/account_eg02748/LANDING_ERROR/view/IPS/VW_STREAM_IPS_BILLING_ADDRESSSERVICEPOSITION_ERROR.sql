CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_BILLING_ADDRESSSERVICEPOSITION_ERROR AS SELECT src, 'IPS_BILLING_ADDRESSSERVICEPOSITION' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDRESSSERVICEKEY), '0'), 38, 10) is null and 
                    src:ADDRESSSERVICEKEY is not null) THEN
                    'ADDRESSSERVICEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ADDRESSSERVICEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDRKEY), '0'), 38, 10) is null and 
                    src:ADDRKEY is not null) THEN
                    'ADDRKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ADDRKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDRSRVPOSITIONKEY), '0'), 38, 10) is null and 
                    src:ADDRSRVPOSITIONKEY is not null) THEN
                    'ADDRSRVPOSITIONKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ADDRSRVPOSITIONKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ASSETTYPE), '0'), 38, 10) is null and 
                    src:ASSETTYPE is not null) THEN
                    'ASSETTYPE contains a non-numeric value : \'' || AS_VARCHAR(src:ASSETTYPE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CONSUMPTIONPERCENTAGE), '0'), 38, 10) is null and 
                    src:CONSUMPTIONPERCENTAGE is not null) THEN
                    'CONSUMPTIONPERCENTAGE contains a non-numeric value : \'' || AS_VARCHAR(src:CONSUMPTIONPERCENTAGE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:METERREGISTERUSE), '0'), 38, 10) is null and 
                    src:METERREGISTERUSE is not null) THEN
                    'METERREGISTERUSE contains a non-numeric value : \'' || AS_VARCHAR(src:METERREGISTERUSE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:POSITION), '0'), 38, 10) is null and 
                    src:POSITION is not null) THEN
                    'POSITION contains a non-numeric value : \'' || AS_VARCHAR(src:POSITION) || '\'' WHEN 
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
                                    
                src:ADDRSRVPOSITIONKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_ADDRESSSERVICEPOSITION as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDRESSSERVICEKEY), '0'), 38, 10) is null and 
                    src:ADDRESSSERVICEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDRKEY), '0'), 38, 10) is null and 
                    src:ADDRKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDRSRVPOSITIONKEY), '0'), 38, 10) is null and 
                    src:ADDRSRVPOSITIONKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ASSETTYPE), '0'), 38, 10) is null and 
                    src:ASSETTYPE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CONSUMPTIONPERCENTAGE), '0'), 38, 10) is null and 
                    src:CONSUMPTIONPERCENTAGE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:METERREGISTERUSE), '0'), 38, 10) is null and 
                    src:METERREGISTERUSE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:POSITION), '0'), 38, 10) is null and 
                    src:POSITION is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)