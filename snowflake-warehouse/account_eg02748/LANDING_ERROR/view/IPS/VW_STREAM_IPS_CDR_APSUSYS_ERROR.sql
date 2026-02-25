CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_CDR_APSUSYS_ERROR AS SELECT src, 'IPS_CDR_APSUSYS' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CASHBGTNOKEY), '0'), 38, 10) is null and 
                    src:CASHBGTNOKEY is not null) THEN
                    'CASHBGTNOKEY contains a non-numeric value : \'' || AS_VARCHAR(src:CASHBGTNOKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CDRPRODFAMILY), '0'), 38, 10) is null and 
                    src:CDRPRODFAMILY is not null) THEN
                    'CDRPRODFAMILY contains a non-numeric value : \'' || AS_VARCHAR(src:CDRPRODFAMILY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OVERBGTNOKEY), '0'), 38, 10) is null and 
                    src:OVERBGTNOKEY is not null) THEN
                    'OVERBGTNOKEY contains a non-numeric value : \'' || AS_VARCHAR(src:OVERBGTNOKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PROPERTYOPTION), '0'), 38, 10) is null and 
                    src:PROPERTYOPTION is not null) THEN
                    'PROPERTYOPTION contains a non-numeric value : \'' || AS_VARCHAR(src:PROPERTYOPTION) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REVBGTNOKEY), '0'), 38, 10) is null and 
                    src:REVBGTNOKEY is not null) THEN
                    'REVBGTNOKEY contains a non-numeric value : \'' || AS_VARCHAR(src:REVBGTNOKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RNDBGTNOKEY), '0'), 38, 10) is null and 
                    src:RNDBGTNOKEY is not null) THEN
                    'RNDBGTNOKEY contains a non-numeric value : \'' || AS_VARCHAR(src:RNDBGTNOKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STARTTAB), '0'), 38, 10) is null and 
                    src:STARTTAB is not null) THEN
                    'STARTTAB contains a non-numeric value : \'' || AS_VARCHAR(src:STARTTAB) || '\'' WHEN 
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
                                    
                src:CDRPRODFAMILY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_CDR_APSUSYS as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CASHBGTNOKEY), '0'), 38, 10) is null and 
                    src:CASHBGTNOKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CDRPRODFAMILY), '0'), 38, 10) is null and 
                    src:CDRPRODFAMILY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OVERBGTNOKEY), '0'), 38, 10) is null and 
                    src:OVERBGTNOKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PROPERTYOPTION), '0'), 38, 10) is null and 
                    src:PROPERTYOPTION is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REVBGTNOKEY), '0'), 38, 10) is null and 
                    src:REVBGTNOKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RNDBGTNOKEY), '0'), 38, 10) is null and 
                    src:RNDBGTNOKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STARTTAB), '0'), 38, 10) is null and 
                    src:STARTTAB is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)