CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_CDR_BUILDING_BLDGSTATUSLOG_ERROR AS SELECT src, 'IPS_CDR_BUILDING_BLDGSTATUSLOG' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APBLDGKEY), '0'), 38, 10) is null and 
                    src:APBLDGKEY is not null) THEN
                    'APBLDGKEY contains a non-numeric value : \'' || AS_VARCHAR(src:APBLDGKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APBLDGPROCESSSTATEKEY), '0'), 38, 10) is null and 
                    src:APBLDGPROCESSSTATEKEY is not null) THEN
                    'APBLDGPROCESSSTATEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:APBLDGPROCESSSTATEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APBLDGSTATLOGKEY), '0'), 38, 10) is null and 
                    src:APBLDGSTATLOGKEY is not null) THEN
                    'APBLDGSTATLOGKEY contains a non-numeric value : \'' || AS_VARCHAR(src:APBLDGSTATLOGKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STATUSDTTM), '1900-01-01')) is null and 
                    src:STATUSDTTM is not null) THEN
                    'STATUSDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:STATUSDTTM) || '\'' WHEN 
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
                                    
                src:APBLDGSTATLOGKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_CDR_BUILDING_BLDGSTATUSLOG as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APBLDGKEY), '0'), 38, 10) is null and 
                    src:APBLDGKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APBLDGPROCESSSTATEKEY), '0'), 38, 10) is null and 
                    src:APBLDGPROCESSSTATEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APBLDGSTATLOGKEY), '0'), 38, 10) is null and 
                    src:APBLDGSTATLOGKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STATUSDTTM), '1900-01-01')) is null and 
                    src:STATUSDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)