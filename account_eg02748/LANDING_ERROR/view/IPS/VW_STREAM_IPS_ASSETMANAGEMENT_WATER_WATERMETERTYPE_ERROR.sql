CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_ASSETMANAGEMENT_WATER_WATERMETERTYPE_ERROR AS SELECT src, 'IPS_ASSETMANAGEMENT_WATER_WATERMETERTYPE' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EFFDATE), '1900-01-01')) is null and 
                    src:EFFDATE is not null) THEN
                    'EFFDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:EFFDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EXPDATE), '1900-01-01')) is null and 
                    src:EXPDATE is not null) THEN
                    'EXPDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:EXPDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HIGHREGMULTIPLIER), '0'), 38, 10) is null and 
                    src:HIGHREGMULTIPLIER is not null) THEN
                    'HIGHREGMULTIPLIER contains a non-numeric value : \'' || AS_VARCHAR(src:HIGHREGMULTIPLIER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HIGHREGNUMBERDECIMALS), '0'), 38, 10) is null and 
                    src:HIGHREGNUMBERDECIMALS is not null) THEN
                    'HIGHREGNUMBERDECIMALS contains a non-numeric value : \'' || AS_VARCHAR(src:HIGHREGNUMBERDECIMALS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HIGHREGNUMBERDIALS), '0'), 38, 10) is null and 
                    src:HIGHREGNUMBERDIALS is not null) THEN
                    'HIGHREGNUMBERDIALS contains a non-numeric value : \'' || AS_VARCHAR(src:HIGHREGNUMBERDIALS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LOWREGMULTIPLIER), '0'), 38, 10) is null and 
                    src:LOWREGMULTIPLIER is not null) THEN
                    'LOWREGMULTIPLIER contains a non-numeric value : \'' || AS_VARCHAR(src:LOWREGMULTIPLIER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LOWREGNUMBERDECIMALS), '0'), 38, 10) is null and 
                    src:LOWREGNUMBERDECIMALS is not null) THEN
                    'LOWREGNUMBERDECIMALS contains a non-numeric value : \'' || AS_VARCHAR(src:LOWREGNUMBERDECIMALS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LOWREGNUMBERDIALS), '0'), 38, 10) is null and 
                    src:LOWREGNUMBERDIALS is not null) THEN
                    'LOWREGNUMBERDIALS contains a non-numeric value : \'' || AS_VARCHAR(src:LOWREGNUMBERDIALS) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
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
                                    
                src:CODE  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_ASSETMANAGEMENT_WATER_WATERMETERTYPE as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EFFDATE), '1900-01-01')) is null and 
                    src:EFFDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EXPDATE), '1900-01-01')) is null and 
                    src:EXPDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HIGHREGMULTIPLIER), '0'), 38, 10) is null and 
                    src:HIGHREGMULTIPLIER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HIGHREGNUMBERDECIMALS), '0'), 38, 10) is null and 
                    src:HIGHREGNUMBERDECIMALS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HIGHREGNUMBERDIALS), '0'), 38, 10) is null and 
                    src:HIGHREGNUMBERDIALS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LOWREGMULTIPLIER), '0'), 38, 10) is null and 
                    src:LOWREGMULTIPLIER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LOWREGNUMBERDECIMALS), '0'), 38, 10) is null and 
                    src:LOWREGNUMBERDECIMALS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LOWREGNUMBERDIALS), '0'), 38, 10) is null and 
                    src:LOWREGNUMBERDIALS is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)