CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_RESOURCES_CONTACTCOMMUNICATION_ERROR AS SELECT src, 'IPS_RESOURCES_CONTACTCOMMUNICATION' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACTIVITYKEY), '0'), 38, 10) is null and 
                    src:ACTIVITYKEY is not null) THEN
                    'ACTIVITYKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ACTIVITYKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CONTACTCOMMBODYKEY), '0'), 38, 10) is null and 
                    src:CONTACTCOMMBODYKEY is not null) THEN
                    'CONTACTCOMMBODYKEY contains a non-numeric value : \'' || AS_VARCHAR(src:CONTACTCOMMBODYKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CONTACTCOMMUNICATIONKEY), '0'), 38, 10) is null and 
                    src:CONTACTCOMMUNICATIONKEY is not null) THEN
                    'CONTACTCOMMUNICATIONKEY contains a non-numeric value : \'' || AS_VARCHAR(src:CONTACTCOMMUNICATIONKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CONTACTKEY), '0'), 38, 10) is null and 
                    src:CONTACTKEY is not null) THEN
                    'CONTACTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:CONTACTKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DELIVERYRESULT), '0'), 38, 10) is null and 
                    src:DELIVERYRESULT is not null) THEN
                    'DELIVERYRESULT contains a non-numeric value : \'' || AS_VARCHAR(src:DELIVERYRESULT) || '\'' WHEN 
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
                                    
                src:CONTACTCOMMUNICATIONKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_RESOURCES_CONTACTCOMMUNICATION as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACTIVITYKEY), '0'), 38, 10) is null and 
                    src:ACTIVITYKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CONTACTCOMMBODYKEY), '0'), 38, 10) is null and 
                    src:CONTACTCOMMBODYKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CONTACTCOMMUNICATIONKEY), '0'), 38, 10) is null and 
                    src:CONTACTCOMMUNICATIONKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CONTACTKEY), '0'), 38, 10) is null and 
                    src:CONTACTKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DELIVERYRESULT), '0'), 38, 10) is null and 
                    src:DELIVERYRESULT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)