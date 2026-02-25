CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_WSLASSETSWATER_XWATERHYDRANT_ERROR AS SELECT src, 'IPS_WSLASSETSWATER_XWATERHYDRANT' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPKEY), '0'), 38, 10) is null and 
                    src:COMPKEY is not null) THEN
                    'COMPKEY contains a non-numeric value : \'' || AS_VARCHAR(src:COMPKEY) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RESIDUALPRESSURE), '0'), 38, 10) is null and 
                    src:RESIDUALPRESSURE is not null) THEN
                    'RESIDUALPRESSURE contains a non-numeric value : \'' || AS_VARCHAR(src:RESIDUALPRESSURE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TORQUE), '0'), 38, 10) is null and 
                    src:TORQUE is not null) THEN
                    'TORQUE contains a non-numeric value : \'' || AS_VARCHAR(src:TORQUE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) THEN
                    'VARIATION_ID contains a non-numeric value : \'' || AS_VARCHAR(src:VARIATION_ID) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WARRANTYEND), '1900-01-01')) is null and 
                    src:WARRANTYEND is not null) THEN
                    'WARRANTYEND contains a non-timestamp value : \'' || AS_VARCHAR(src:WARRANTYEND) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WARRANTYSTART), '1900-01-01')) is null and 
                    src:WARRANTYSTART is not null) THEN
                    'WARRANTYSTART contains a non-timestamp value : \'' || AS_VARCHAR(src:WARRANTYSTART) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XWATERHYDRANTKEY), '0'), 38, 10) is null and 
                    src:XWATERHYDRANTKEY is not null) THEN
                    'XWATERHYDRANTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:XWATERHYDRANTKEY) || '\'' WHEN 
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
                                    
                src:XWATERHYDRANTKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_WSLASSETSWATER_XWATERHYDRANT as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPKEY), '0'), 38, 10) is null and 
                    src:COMPKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MAXFLOW), '0'), 38, 10) is null and 
                    src:MAXFLOW is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MINFLOW), '0'), 38, 10) is null and 
                    src:MINFLOW is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PRESSURERATING), '0'), 38, 10) is null and 
                    src:PRESSURERATING is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RESIDUALPRESSURE), '0'), 38, 10) is null and 
                    src:RESIDUALPRESSURE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TORQUE), '0'), 38, 10) is null and 
                    src:TORQUE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WARRANTYEND), '1900-01-01')) is null and 
                    src:WARRANTYEND is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WARRANTYSTART), '1900-01-01')) is null and 
                    src:WARRANTYSTART is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XWATERHYDRANTKEY), '0'), 38, 10) is null and 
                    src:XWATERHYDRANTKEY is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)