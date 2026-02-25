CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_CDR_PLANCOPYREVIEW_ERROR AS SELECT src, 'IPS_CDR_PLANCOPYREVIEW' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APBLDGREVIEWKEY), '0'), 38, 10) is null and 
                    src:APBLDGREVIEWKEY is not null) THEN
                    'APBLDGREVIEWKEY contains a non-numeric value : \'' || AS_VARCHAR(src:APBLDGREVIEWKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPLANREVIEWKEY), '0'), 38, 10) is null and 
                    src:APPLANREVIEWKEY is not null) THEN
                    'APPLANREVIEWKEY contains a non-numeric value : \'' || AS_VARCHAR(src:APPLANREVIEWKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPROJREVIEWKEY), '0'), 38, 10) is null and 
                    src:APPROJREVIEWKEY is not null) THEN
                    'APPROJREVIEWKEY contains a non-numeric value : \'' || AS_VARCHAR(src:APPROJREVIEWKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APUSEREVIEWKEY), '0'), 38, 10) is null and 
                    src:APUSEREVIEWKEY is not null) THEN
                    'APUSEREVIEWKEY contains a non-numeric value : \'' || AS_VARCHAR(src:APUSEREVIEWKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PLANCOPYKEY), '0'), 38, 10) is null and 
                    src:PLANCOPYKEY is not null) THEN
                    'PLANCOPYKEY contains a non-numeric value : \'' || AS_VARCHAR(src:PLANCOPYKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PLANCOPYREVIEWKEY), '0'), 38, 10) is null and 
                    src:PLANCOPYREVIEWKEY is not null) THEN
                    'PLANCOPYREVIEWKEY contains a non-numeric value : \'' || AS_VARCHAR(src:PLANCOPYREVIEWKEY) || '\'' WHEN 
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
                                    
                src:PLANCOPYREVIEWKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_CDR_PLANCOPYREVIEW as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APBLDGREVIEWKEY), '0'), 38, 10) is null and 
                    src:APBLDGREVIEWKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPLANREVIEWKEY), '0'), 38, 10) is null and 
                    src:APPLANREVIEWKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPROJREVIEWKEY), '0'), 38, 10) is null and 
                    src:APPROJREVIEWKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APUSEREVIEWKEY), '0'), 38, 10) is null and 
                    src:APUSEREVIEWKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PLANCOPYKEY), '0'), 38, 10) is null and 
                    src:PLANCOPYKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PLANCOPYREVIEWKEY), '0'), 38, 10) is null and 
                    src:PLANCOPYREVIEWKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)