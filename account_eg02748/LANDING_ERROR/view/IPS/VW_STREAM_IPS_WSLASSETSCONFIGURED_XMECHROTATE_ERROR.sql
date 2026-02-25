CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_WSLASSETSCONFIGURED_XMECHROTATE_ERROR AS SELECT src, 'IPS_WSLASSETSCONFIGURED_XMECHROTATE' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPKEY), '0'), 38, 10) is null and 
                    src:COMPKEY is not null) THEN
                    'COMPKEY contains a non-numeric value : \'' || AS_VARCHAR(src:COMPKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CURRENTIN), '0'), 38, 10) is null and 
                    src:CURRENTIN is not null) THEN
                    'CURRENTIN contains a non-numeric value : \'' || AS_VARCHAR(src:CURRENTIN) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DESIGNSPEED), '0'), 38, 10) is null and 
                    src:DESIGNSPEED is not null) THEN
                    'DESIGNSPEED contains a non-numeric value : \'' || AS_VARCHAR(src:DESIGNSPEED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DIAMINLET), '0'), 38, 10) is null and 
                    src:DIAMINLET is not null) THEN
                    'DIAMINLET contains a non-numeric value : \'' || AS_VARCHAR(src:DIAMINLET) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DIAMOUTLET), '0'), 38, 10) is null and 
                    src:DIAMOUTLET is not null) THEN
                    'DIAMOUTLET contains a non-numeric value : \'' || AS_VARCHAR(src:DIAMOUTLET) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ENERGYRATING), '0'), 38, 10) is null and 
                    src:ENERGYRATING is not null) THEN
                    'ENERGYRATING contains a non-numeric value : \'' || AS_VARCHAR(src:ENERGYRATING) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:IMPELLORDIA), '0'), 38, 10) is null and 
                    src:IMPELLORDIA is not null) THEN
                    'IMPELLORDIA contains a non-numeric value : \'' || AS_VARCHAR(src:IMPELLORDIA) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MAXFLOW), '0'), 38, 10) is null and 
                    src:MAXFLOW is not null) THEN
                    'MAXFLOW contains a non-numeric value : \'' || AS_VARCHAR(src:MAXFLOW) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MINFLOW), '0'), 38, 10) is null and 
                    src:MINFLOW is not null) THEN
                    'MINFLOW contains a non-numeric value : \'' || AS_VARCHAR(src:MINFLOW) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PRESSURERATING), '0'), 38, 10) is null and 
                    src:PRESSURERATING is not null) THEN
                    'PRESSURERATING contains a non-numeric value : \'' || AS_VARCHAR(src:PRESSURERATING) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STAGES), '0'), 38, 10) is null and 
                    src:STAGES is not null) THEN
                    'STAGES contains a non-numeric value : \'' || AS_VARCHAR(src:STAGES) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TORQUE), '0'), 38, 10) is null and 
                    src:TORQUE is not null) THEN
                    'TORQUE contains a non-numeric value : \'' || AS_VARCHAR(src:TORQUE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TORQUEOUT), '0'), 38, 10) is null and 
                    src:TORQUEOUT is not null) THEN
                    'TORQUEOUT contains a non-numeric value : \'' || AS_VARCHAR(src:TORQUEOUT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) THEN
                    'VARIATION_ID contains a non-numeric value : \'' || AS_VARCHAR(src:VARIATION_ID) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VOLTAGEIN), '0'), 38, 10) is null and 
                    src:VOLTAGEIN is not null) THEN
                    'VOLTAGEIN contains a non-numeric value : \'' || AS_VARCHAR(src:VOLTAGEIN) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VOLTAGEOUT), '0'), 38, 10) is null and 
                    src:VOLTAGEOUT is not null) THEN
                    'VOLTAGEOUT contains a non-numeric value : \'' || AS_VARCHAR(src:VOLTAGEOUT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WARRANTYEND), '1900-01-01')) is null and 
                    src:WARRANTYEND is not null) THEN
                    'WARRANTYEND contains a non-timestamp value : \'' || AS_VARCHAR(src:WARRANTYEND) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WARRANTYSTART), '1900-01-01')) is null and 
                    src:WARRANTYSTART is not null) THEN
                    'WARRANTYSTART contains a non-timestamp value : \'' || AS_VARCHAR(src:WARRANTYSTART) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XMECHROTATEKEY), '0'), 38, 10) is null and 
                    src:XMECHROTATEKEY is not null) THEN
                    'XMECHROTATEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:XMECHROTATEKEY) || '\'' WHEN 
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
                                    
                src:XMECHROTATEKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_WSLASSETSCONFIGURED_XMECHROTATE as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPKEY), '0'), 38, 10) is null and 
                    src:COMPKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CURRENTIN), '0'), 38, 10) is null and 
                    src:CURRENTIN is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DESIGNSPEED), '0'), 38, 10) is null and 
                    src:DESIGNSPEED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DIAMINLET), '0'), 38, 10) is null and 
                    src:DIAMINLET is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DIAMOUTLET), '0'), 38, 10) is null and 
                    src:DIAMOUTLET is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ENERGYRATING), '0'), 38, 10) is null and 
                    src:ENERGYRATING is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:IMPELLORDIA), '0'), 38, 10) is null and 
                    src:IMPELLORDIA is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MAXFLOW), '0'), 38, 10) is null and 
                    src:MAXFLOW is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MINFLOW), '0'), 38, 10) is null and 
                    src:MINFLOW is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PRESSURERATING), '0'), 38, 10) is null and 
                    src:PRESSURERATING is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STAGES), '0'), 38, 10) is null and 
                    src:STAGES is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TORQUE), '0'), 38, 10) is null and 
                    src:TORQUE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TORQUEOUT), '0'), 38, 10) is null and 
                    src:TORQUEOUT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VOLTAGEIN), '0'), 38, 10) is null and 
                    src:VOLTAGEIN is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VOLTAGEOUT), '0'), 38, 10) is null and 
                    src:VOLTAGEOUT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WARRANTYEND), '1900-01-01')) is null and 
                    src:WARRANTYEND is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WARRANTYSTART), '1900-01-01')) is null and 
                    src:WARRANTYSTART is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XMECHROTATEKEY), '0'), 38, 10) is null and 
                    src:XMECHROTATEKEY is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)