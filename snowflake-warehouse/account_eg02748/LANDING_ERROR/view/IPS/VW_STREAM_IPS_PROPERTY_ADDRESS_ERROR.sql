CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_PROPERTY_ADDRESS_ERROR AS SELECT src, 'IPS_PROPERTY_ADDRESS' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDRKEY), '0'), 38, 10) is null and 
                    src:ADDRKEY is not null) THEN
                    'ADDRKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ADDRKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CASSVALIDATIONDT), '1900-01-01')) is null and 
                    src:CASSVALIDATIONDT is not null) THEN
                    'CASSVALIDATIONDT contains a non-timestamp value : \'' || AS_VARCHAR(src:CASSVALIDATIONDT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EFFDATE), '1900-01-01')) is null and 
                    src:EFFDATE is not null) THEN
                    'EFFDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:EFFDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EXPDATE), '1900-01-01')) is null and 
                    src:EXPDATE is not null) THEN
                    'EXPDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:EXPDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:GPSX), '0'), 38, 10) is null and 
                    src:GPSX is not null) THEN
                    'GPSX contains a non-numeric value : \'' || AS_VARCHAR(src:GPSX) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:GPSY), '0'), 38, 10) is null and 
                    src:GPSY is not null) THEN
                    'GPSY contains a non-numeric value : \'' || AS_VARCHAR(src:GPSY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:GPSZ), '0'), 38, 10) is null and 
                    src:GPSZ is not null) THEN
                    'GPSZ contains a non-numeric value : \'' || AS_VARCHAR(src:GPSZ) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MANAGEMENTGROUP), '0'), 38, 10) is null and 
                    src:MANAGEMENTGROUP is not null) THEN
                    'MANAGEMENTGROUP contains a non-numeric value : \'' || AS_VARCHAR(src:MANAGEMENTGROUP) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NZHNOSORTAS), '0'), 38, 10) is null and 
                    src:NZHNOSORTAS is not null) THEN
                    'NZHNOSORTAS contains a non-numeric value : \'' || AS_VARCHAR(src:NZHNOSORTAS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NZHNOSORTASHI), '0'), 38, 10) is null and 
                    src:NZHNOSORTASHI is not null) THEN
                    'NZHNOSORTASHI contains a non-numeric value : \'' || AS_VARCHAR(src:NZHNOSORTASHI) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OPTD), '0'), 38, 10) is null and 
                    src:OPTD is not null) THEN
                    'OPTD contains a non-numeric value : \'' || AS_VARCHAR(src:OPTD) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) THEN
                    'VARIATION_ID contains a non-numeric value : \'' || AS_VARCHAR(src:VARIATION_ID) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VERSION), '0'), 38, 10) is null and 
                    src:VERSION is not null) THEN
                    'VERSION contains a non-numeric value : \'' || AS_VARCHAR(src:VERSION) || '\'' WHEN 
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
                                    
                src:ADDRKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_PROPERTY_ADDRESS as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDRKEY), '0'), 38, 10) is null and 
                    src:ADDRKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CASSVALIDATIONDT), '1900-01-01')) is null and 
                    src:CASSVALIDATIONDT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EFFDATE), '1900-01-01')) is null and 
                    src:EFFDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EXPDATE), '1900-01-01')) is null and 
                    src:EXPDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:GPSX), '0'), 38, 10) is null and 
                    src:GPSX is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:GPSY), '0'), 38, 10) is null and 
                    src:GPSY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:GPSZ), '0'), 38, 10) is null and 
                    src:GPSZ is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MANAGEMENTGROUP), '0'), 38, 10) is null and 
                    src:MANAGEMENTGROUP is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NZHNOSORTAS), '0'), 38, 10) is null and 
                    src:NZHNOSORTAS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NZHNOSORTASHI), '0'), 38, 10) is null and 
                    src:NZHNOSORTASHI is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OPTD), '0'), 38, 10) is null and 
                    src:OPTD is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VERSION), '0'), 38, 10) is null and 
                    src:VERSION is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)