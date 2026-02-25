CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_WSLINTERFACE_QUOTEGENERATIONSTAGING_ERROR AS SELECT src, 'IPS_WSLINTERFACE_QUOTEGENERATIONSTAGING' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:APDTTM), '1900-01-01')) is null and 
                    src:APDTTM is not null) THEN
                    'APDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:APDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DUEDAYS), '0'), 38, 10) is null and 
                    src:DUEDAYS is not null) THEN
                    'DUEDAYS contains a non-numeric value : \'' || AS_VARCHAR(src:DUEDAYS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:GST), '0'), 38, 10) is null and 
                    src:GST is not null) THEN
                    'GST contains a non-numeric value : \'' || AS_VARCHAR(src:GST) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:KEYCOLUMN), '0'), 38, 10) is null and 
                    src:KEYCOLUMN is not null) THEN
                    'KEYCOLUMN contains a non-numeric value : \'' || AS_VARCHAR(src:KEYCOLUMN) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:QUOTEGENERATIONSTAGINGKEY), '0'), 38, 10) is null and 
                    src:QUOTEGENERATIONSTAGINGKEY is not null) THEN
                    'QUOTEGENERATIONSTAGINGKEY contains a non-numeric value : \'' || AS_VARCHAR(src:QUOTEGENERATIONSTAGINGKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TOTALAFTERTAX), '0'), 38, 10) is null and 
                    src:TOTALAFTERTAX is not null) THEN
                    'TOTALAFTERTAX contains a non-numeric value : \'' || AS_VARCHAR(src:TOTALAFTERTAX) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TOTALVALUE), '0'), 38, 10) is null and 
                    src:TOTALVALUE is not null) THEN
                    'TOTALVALUE contains a non-numeric value : \'' || AS_VARCHAR(src:TOTALVALUE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VALIDTILL), '1900-01-01')) is null and 
                    src:VALIDTILL is not null) THEN
                    'VALIDTILL contains a non-timestamp value : \'' || AS_VARCHAR(src:VALIDTILL) || '\'' WHEN 
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
                                    
                src:QUOTEGENERATIONSTAGINGKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_WSLINTERFACE_QUOTEGENERATIONSTAGING as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:APDTTM), '1900-01-01')) is null and 
                    src:APDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DUEDAYS), '0'), 38, 10) is null and 
                    src:DUEDAYS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:GST), '0'), 38, 10) is null and 
                    src:GST is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:KEYCOLUMN), '0'), 38, 10) is null and 
                    src:KEYCOLUMN is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:QUOTEGENERATIONSTAGINGKEY), '0'), 38, 10) is null and 
                    src:QUOTEGENERATIONSTAGINGKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TOTALAFTERTAX), '0'), 38, 10) is null and 
                    src:TOTALAFTERTAX is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TOTALVALUE), '0'), 38, 10) is null and 
                    src:TOTALVALUE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VALIDTILL), '1900-01-01')) is null and 
                    src:VALIDTILL is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)