CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_RESOURCES_RATETABLE_RATETBL_ERROR AS SELECT src, 'IPS_RESOURCES_RATETABLE_RATETBL' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CYCLEDAYS), '0'), 38, 10) is null and 
                    src:CYCLEDAYS is not null) THEN
                    'CYCLEDAYS contains a non-numeric value : \'' || AS_VARCHAR(src:CYCLEDAYS) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EFFDATE), '1900-01-01')) is null and 
                    src:EFFDATE is not null) THEN
                    'EFFDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:EFFDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FINALCYCLEDAYS), '0'), 38, 10) is null and 
                    src:FINALCYCLEDAYS is not null) THEN
                    'FINALCYCLEDAYS contains a non-numeric value : \'' || AS_VARCHAR(src:FINALCYCLEDAYS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FORMULA), '0'), 38, 10) is null and 
                    src:FORMULA is not null) THEN
                    'FORMULA contains a non-numeric value : \'' || AS_VARCHAR(src:FORMULA) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MAXIMUMCHARGE), '0'), 38, 10) is null and 
                    src:MAXIMUMCHARGE is not null) THEN
                    'MAXIMUMCHARGE contains a non-numeric value : \'' || AS_VARCHAR(src:MAXIMUMCHARGE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MINIMUMCHARGE), '0'), 38, 10) is null and 
                    src:MINIMUMCHARGE is not null) THEN
                    'MINIMUMCHARGE contains a non-numeric value : \'' || AS_VARCHAR(src:MINIMUMCHARGE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:NEWEFFECTIVEDATE), '1900-01-01')) is null and 
                    src:NEWEFFECTIVEDATE is not null) THEN
                    'NEWEFFECTIVEDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:NEWEFFECTIVEDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMBEROFDECIMALS), '0'), 38, 10) is null and 
                    src:NUMBEROFDECIMALS is not null) THEN
                    'NUMBEROFDECIMALS contains a non-numeric value : \'' || AS_VARCHAR(src:NUMBEROFDECIMALS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PERCENTINCREASE), '0'), 38, 10) is null and 
                    src:PERCENTINCREASE is not null) THEN
                    'PERCENTINCREASE contains a non-numeric value : \'' || AS_VARCHAR(src:PERCENTINCREASE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RATECODEKEY), '0'), 38, 10) is null and 
                    src:RATECODEKEY is not null) THEN
                    'RATECODEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:RATECODEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RATETBLKEY), '0'), 38, 10) is null and 
                    src:RATETBLKEY is not null) THEN
                    'RATETBLKEY contains a non-numeric value : \'' || AS_VARCHAR(src:RATETBLKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USEFORPRORATING), '0'), 38, 10) is null and 
                    src:USEFORPRORATING is not null) THEN
                    'USEFORPRORATING contains a non-numeric value : \'' || AS_VARCHAR(src:USEFORPRORATING) || '\'' WHEN 
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
                                    
                src:RATETBLKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_RESOURCES_RATETABLE_RATETBL as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CYCLEDAYS), '0'), 38, 10) is null and 
                    src:CYCLEDAYS is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EFFDATE), '1900-01-01')) is null and 
                    src:EFFDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FINALCYCLEDAYS), '0'), 38, 10) is null and 
                    src:FINALCYCLEDAYS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FORMULA), '0'), 38, 10) is null and 
                    src:FORMULA is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MAXIMUMCHARGE), '0'), 38, 10) is null and 
                    src:MAXIMUMCHARGE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MINIMUMCHARGE), '0'), 38, 10) is null and 
                    src:MINIMUMCHARGE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:NEWEFFECTIVEDATE), '1900-01-01')) is null and 
                    src:NEWEFFECTIVEDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMBEROFDECIMALS), '0'), 38, 10) is null and 
                    src:NUMBEROFDECIMALS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PERCENTINCREASE), '0'), 38, 10) is null and 
                    src:PERCENTINCREASE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RATECODEKEY), '0'), 38, 10) is null and 
                    src:RATECODEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RATETBLKEY), '0'), 38, 10) is null and 
                    src:RATETBLKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USEFORPRORATING), '0'), 38, 10) is null and 
                    src:USEFORPRORATING is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)