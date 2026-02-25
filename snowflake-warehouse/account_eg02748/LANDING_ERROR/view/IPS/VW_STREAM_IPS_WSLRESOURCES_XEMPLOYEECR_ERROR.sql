CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_WSLRESOURCES_XEMPLOYEECR_ERROR AS SELECT src, 'IPS_WSLRESOURCES_XEMPLOYEECR' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CAPACITY), '0'), 38, 10) is null and 
                    src:CAPACITY is not null) THEN
                    'CAPACITY contains a non-numeric value : \'' || AS_VARCHAR(src:CAPACITY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CAPACITYRATE), '0'), 38, 10) is null and 
                    src:CAPACITYRATE is not null) THEN
                    'CAPACITYRATE contains a non-numeric value : \'' || AS_VARCHAR(src:CAPACITYRATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MULTIDOMCAPACITY), '0'), 38, 10) is null and 
                    src:MULTIDOMCAPACITY is not null) THEN
                    'MULTIDOMCAPACITY contains a non-numeric value : \'' || AS_VARCHAR(src:MULTIDOMCAPACITY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) THEN
                    'VARIATION_ID contains a non-numeric value : \'' || AS_VARCHAR(src:VARIATION_ID) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XEMPLOYEECRKEY), '0'), 38, 10) is null and 
                    src:XEMPLOYEECRKEY is not null) THEN
                    'XEMPLOYEECRKEY contains a non-numeric value : \'' || AS_VARCHAR(src:XEMPLOYEECRKEY) || '\'' WHEN 
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
                                    
                src:XEMPLOYEECRKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_WSLRESOURCES_XEMPLOYEECR as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CAPACITY), '0'), 38, 10) is null and 
                    src:CAPACITY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CAPACITYRATE), '0'), 38, 10) is null and 
                    src:CAPACITYRATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MULTIDOMCAPACITY), '0'), 38, 10) is null and 
                    src:MULTIDOMCAPACITY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XEMPLOYEECRKEY), '0'), 38, 10) is null and 
                    src:XEMPLOYEECRKEY is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)