CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_PORTAL_MEMBERSHIPLOG_ERROR AS SELECT src, 'IPS_PORTAL_MEMBERSHIPLOG' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MEMBERSHIP), '0'), 38, 10) is null and 
                    src:MEMBERSHIP is not null) THEN
                    'MEMBERSHIP contains a non-numeric value : \'' || AS_VARCHAR(src:MEMBERSHIP) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MEMBERSHIPACTIVITY), '0'), 38, 10) is null and 
                    src:MEMBERSHIPACTIVITY is not null) THEN
                    'MEMBERSHIPACTIVITY contains a non-numeric value : \'' || AS_VARCHAR(src:MEMBERSHIPACTIVITY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MEMBERSHIPLOGKEY), '0'), 38, 10) is null and 
                    src:MEMBERSHIPLOGKEY is not null) THEN
                    'MEMBERSHIPLOGKEY contains a non-numeric value : \'' || AS_VARCHAR(src:MEMBERSHIPLOGKEY) || '\'' WHEN 
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
                                    
                src:MEMBERSHIPLOGKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_PORTAL_MEMBERSHIPLOG as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MEMBERSHIP), '0'), 38, 10) is null and 
                    src:MEMBERSHIP is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MEMBERSHIPACTIVITY), '0'), 38, 10) is null and 
                    src:MEMBERSHIPACTIVITY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MEMBERSHIPLOGKEY), '0'), 38, 10) is null and 
                    src:MEMBERSHIPLOGKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)