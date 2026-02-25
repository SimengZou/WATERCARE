CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_WSLCDRBUILD_XBUILDINSPMETERDP_ERROR AS SELECT src, 'IPS_WSLCDRBUILD_XBUILDINSPMETERDP' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APBLDGINSPDTLKEY), '0'), 38, 10) is null and 
                    src:APBLDGINSPDTLKEY is not null) THEN
                    'APBLDGINSPDTLKEY contains a non-numeric value : \'' || AS_VARCHAR(src:APBLDGINSPDTLKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ESTTOTAL), '0'), 38, 10) is null and 
                    src:ESTTOTAL is not null) THEN
                    'ESTTOTAL contains a non-numeric value : \'' || AS_VARCHAR(src:ESTTOTAL) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:FIREMETERINSTALLDATE), '1900-01-01')) is null and 
                    src:FIREMETERINSTALLDATE is not null) THEN
                    'FIREMETERINSTALLDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:FIREMETERINSTALLDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PEAKFLOW), '0'), 38, 10) is null and 
                    src:PEAKFLOW is not null) THEN
                    'PEAKFLOW contains a non-numeric value : \'' || AS_VARCHAR(src:PEAKFLOW) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SPRINKLERSUPPLYPIPESIZE), '0'), 38, 10) is null and 
                    src:SPRINKLERSUPPLYPIPESIZE is not null) THEN
                    'SPRINKLERSUPPLYPIPESIZE contains a non-numeric value : \'' || AS_VARCHAR(src:SPRINKLERSUPPLYPIPESIZE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) THEN
                    'VARIATION_ID contains a non-numeric value : \'' || AS_VARCHAR(src:VARIATION_ID) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WATERMAINSIZE), '0'), 38, 10) is null and 
                    src:WATERMAINSIZE is not null) THEN
                    'WATERMAINSIZE contains a non-numeric value : \'' || AS_VARCHAR(src:WATERMAINSIZE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XBUILDINSPMETERDPKEY), '0'), 38, 10) is null and 
                    src:XBUILDINSPMETERDPKEY is not null) THEN
                    'XBUILDINSPMETERDPKEY contains a non-numeric value : \'' || AS_VARCHAR(src:XBUILDINSPMETERDPKEY) || '\'' WHEN 
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
                                    
                src:XBUILDINSPMETERDPKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_WSLCDRBUILD_XBUILDINSPMETERDP as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APBLDGINSPDTLKEY), '0'), 38, 10) is null and 
                    src:APBLDGINSPDTLKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ESTTOTAL), '0'), 38, 10) is null and 
                    src:ESTTOTAL is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:FIREMETERINSTALLDATE), '1900-01-01')) is null and 
                    src:FIREMETERINSTALLDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PEAKFLOW), '0'), 38, 10) is null and 
                    src:PEAKFLOW is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SPRINKLERSUPPLYPIPESIZE), '0'), 38, 10) is null and 
                    src:SPRINKLERSUPPLYPIPESIZE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WATERMAINSIZE), '0'), 38, 10) is null and 
                    src:WATERMAINSIZE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XBUILDINSPMETERDPKEY), '0'), 38, 10) is null and 
                    src:XBUILDINSPMETERDPKEY is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)