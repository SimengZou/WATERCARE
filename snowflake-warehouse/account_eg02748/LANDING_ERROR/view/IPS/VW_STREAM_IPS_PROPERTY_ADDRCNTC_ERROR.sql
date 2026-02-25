CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_PROPERTY_ADDRCNTC_ERROR AS SELECT src, 'IPS_PROPERTY_ADDRCNTC' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDRCNTCKEY), '0'), 38, 10) is null and 
                    src:ADDRCNTCKEY is not null) THEN
                    'ADDRCNTCKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ADDRCNTCKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDRKEY), '0'), 38, 10) is null and 
                    src:ADDRKEY is not null) THEN
                    'ADDRKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ADDRKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CNTCTKEY), '0'), 38, 10) is null and 
                    src:CNTCTKEY is not null) THEN
                    'CNTCTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:CNTCTKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OCCFROMDT), '1900-01-01')) is null and 
                    src:OCCFROMDT is not null) THEN
                    'OCCFROMDT contains a non-timestamp value : \'' || AS_VARCHAR(src:OCCFROMDT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OCCTODT), '1900-01-01')) is null and 
                    src:OCCTODT is not null) THEN
                    'OCCTODT contains a non-timestamp value : \'' || AS_VARCHAR(src:OCCTODT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OWNFROMDT), '1900-01-01')) is null and 
                    src:OWNFROMDT is not null) THEN
                    'OWNFROMDT contains a non-timestamp value : \'' || AS_VARCHAR(src:OWNFROMDT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OWNTODT), '1900-01-01')) is null and 
                    src:OWNTODT is not null) THEN
                    'OWNTODT contains a non-timestamp value : \'' || AS_VARCHAR(src:OWNTODT) || '\'' WHEN 
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
                                    
                src:ADDRCNTCKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_PROPERTY_ADDRCNTC as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDRCNTCKEY), '0'), 38, 10) is null and 
                    src:ADDRCNTCKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDRKEY), '0'), 38, 10) is null and 
                    src:ADDRKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CNTCTKEY), '0'), 38, 10) is null and 
                    src:CNTCTKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OCCFROMDT), '1900-01-01')) is null and 
                    src:OCCFROMDT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OCCTODT), '1900-01-01')) is null and 
                    src:OCCTODT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OWNFROMDT), '1900-01-01')) is null and 
                    src:OWNFROMDT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OWNTODT), '1900-01-01')) is null and 
                    src:OWNTODT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)