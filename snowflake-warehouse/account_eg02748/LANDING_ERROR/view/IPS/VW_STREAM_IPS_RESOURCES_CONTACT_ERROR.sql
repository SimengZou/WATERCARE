CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_RESOURCES_CONTACT_ERROR AS SELECT src, 'IPS_RESOURCES_CONTACT' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CASSBARCODE), '0'), 38, 10) is null and 
                    src:CASSBARCODE is not null) THEN
                    'CASSBARCODE contains a non-numeric value : \'' || AS_VARCHAR(src:CASSBARCODE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CASSVALIDATIONDT), '1900-01-01')) is null and 
                    src:CASSVALIDATIONDT is not null) THEN
                    'CASSVALIDATIONDT contains a non-timestamp value : \'' || AS_VARCHAR(src:CASSVALIDATIONDT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CASSVER), '0'), 38, 10) is null and 
                    src:CASSVER is not null) THEN
                    'CASSVER contains a non-numeric value : \'' || AS_VARCHAR(src:CASSVER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CNTCTKEY), '0'), 38, 10) is null and 
                    src:CNTCTKEY is not null) THEN
                    'CNTCTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:CNTCTKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CORRDELIVERY), '0'), 38, 10) is null and 
                    src:CORRDELIVERY is not null) THEN
                    'CORRDELIVERY contains a non-numeric value : \'' || AS_VARCHAR(src:CORRDELIVERY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EXPDATE), '1900-01-01')) is null and 
                    src:EXPDATE is not null) THEN
                    'EXPDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:EXPDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:IDKEY), '0'), 38, 10) is null and 
                    src:IDKEY is not null) THEN
                    'IDKEY contains a non-numeric value : \'' || AS_VARCHAR(src:IDKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SALARY), '0'), 38, 10) is null and 
                    src:SALARY is not null) THEN
                    'SALARY contains a non-numeric value : \'' || AS_VARCHAR(src:SALARY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SEASCASSBARCODE), '0'), 38, 10) is null and 
                    src:SEASCASSBARCODE is not null) THEN
                    'SEASCASSBARCODE contains a non-numeric value : \'' || AS_VARCHAR(src:SEASCASSBARCODE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SEASCASSVALIDATIONDT), '1900-01-01')) is null and 
                    src:SEASCASSVALIDATIONDT is not null) THEN
                    'SEASCASSVALIDATIONDT contains a non-timestamp value : \'' || AS_VARCHAR(src:SEASCASSVALIDATIONDT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SEASCASSVER), '0'), 38, 10) is null and 
                    src:SEASCASSVER is not null) THEN
                    'SEASCASSVER contains a non-numeric value : \'' || AS_VARCHAR(src:SEASCASSVER) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SEASFROMDT), '1900-01-01')) is null and 
                    src:SEASFROMDT is not null) THEN
                    'SEASFROMDT contains a non-timestamp value : \'' || AS_VARCHAR(src:SEASFROMDT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SEASTODT), '1900-01-01')) is null and 
                    src:SEASTODT is not null) THEN
                    'SEASTODT contains a non-timestamp value : \'' || AS_VARCHAR(src:SEASTODT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) THEN
                    'VARIATION_ID contains a non-numeric value : \'' || AS_VARCHAR(src:VARIATION_ID) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WEBUSERKEY), '0'), 38, 10) is null and 
                    src:WEBUSERKEY is not null) THEN
                    'WEBUSERKEY contains a non-numeric value : \'' || AS_VARCHAR(src:WEBUSERKEY) || '\'' WHEN 
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
                                    
                src:CNTCTKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_RESOURCES_CONTACT as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CASSBARCODE), '0'), 38, 10) is null and 
                    src:CASSBARCODE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CASSVALIDATIONDT), '1900-01-01')) is null and 
                    src:CASSVALIDATIONDT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CASSVER), '0'), 38, 10) is null and 
                    src:CASSVER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CNTCTKEY), '0'), 38, 10) is null and 
                    src:CNTCTKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CORRDELIVERY), '0'), 38, 10) is null and 
                    src:CORRDELIVERY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EXPDATE), '1900-01-01')) is null and 
                    src:EXPDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:IDKEY), '0'), 38, 10) is null and 
                    src:IDKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SALARY), '0'), 38, 10) is null and 
                    src:SALARY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SEASCASSBARCODE), '0'), 38, 10) is null and 
                    src:SEASCASSBARCODE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SEASCASSVALIDATIONDT), '1900-01-01')) is null and 
                    src:SEASCASSVALIDATIONDT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SEASCASSVER), '0'), 38, 10) is null and 
                    src:SEASCASSVER is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SEASFROMDT), '1900-01-01')) is null and 
                    src:SEASFROMDT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SEASTODT), '1900-01-01')) is null and 
                    src:SEASTODT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WEBUSERKEY), '0'), 38, 10) is null and 
                    src:WEBUSERKEY is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)