CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_BILLING_BUSINESSTYPESERVICEDEFINITIONS_ERROR AS SELECT src, 'IPS_BILLING_BUSINESSTYPESERVICEDEFINITIONS' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BUSINESSTYPESRVDEFNKEY), '0'), 38, 10) is null and 
                    src:BUSINESSTYPESRVDEFNKEY is not null) THEN
                    'BUSINESSTYPESRVDEFNKEY contains a non-numeric value : \'' || AS_VARCHAR(src:BUSINESSTYPESRVDEFNKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DEFAULTAVERAGEUSAGE), '0'), 38, 10) is null and 
                    src:DEFAULTAVERAGEUSAGE is not null) THEN
                    'DEFAULTAVERAGEUSAGE contains a non-numeric value : \'' || AS_VARCHAR(src:DEFAULTAVERAGEUSAGE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SERVICEDEFINITIONKEY), '0'), 38, 10) is null and 
                    src:SERVICEDEFINITIONKEY is not null) THEN
                    'SERVICEDEFINITIONKEY contains a non-numeric value : \'' || AS_VARCHAR(src:SERVICEDEFINITIONKEY) || '\'' WHEN 
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
                                    
                src:BUSINESSTYPESRVDEFNKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_BUSINESSTYPESERVICEDEFINITIONS as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BUSINESSTYPESRVDEFNKEY), '0'), 38, 10) is null and 
                    src:BUSINESSTYPESRVDEFNKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DEFAULTAVERAGEUSAGE), '0'), 38, 10) is null and 
                    src:DEFAULTAVERAGEUSAGE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SERVICEDEFINITIONKEY), '0'), 38, 10) is null and 
                    src:SERVICEDEFINITIONKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)