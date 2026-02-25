CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_CDR_BUILDING_BLDGVLTN_ERROR AS SELECT src, 'IPS_CDR_BUILDING_BLDGVLTN' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APBLDGKEY), '0'), 38, 10) is null and 
                    src:APBLDGKEY is not null) THEN
                    'APBLDGKEY contains a non-numeric value : \'' || AS_VARCHAR(src:APBLDGKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APBLDGVLTNKEY), '0'), 38, 10) is null and 
                    src:APBLDGVLTNKEY is not null) THEN
                    'APBLDGVLTNKEY contains a non-numeric value : \'' || AS_VARCHAR(src:APBLDGVLTNKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BLDGAREA), '0'), 38, 10) is null and 
                    src:BLDGAREA is not null) THEN
                    'BLDGAREA contains a non-numeric value : \'' || AS_VARCHAR(src:BLDGAREA) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CALCVLTN), '0'), 38, 10) is null and 
                    src:CALCVLTN is not null) THEN
                    'CALCVLTN contains a non-numeric value : \'' || AS_VARCHAR(src:CALCVLTN) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MULT), '0'), 38, 10) is null and 
                    src:MULT is not null) THEN
                    'MULT contains a non-numeric value : \'' || AS_VARCHAR(src:MULT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SUVLTNKEY), '0'), 38, 10) is null and 
                    src:SUVLTNKEY is not null) THEN
                    'SUVLTNKEY contains a non-numeric value : \'' || AS_VARCHAR(src:SUVLTNKEY) || '\'' WHEN 
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
                                    
                src:APBLDGVLTNKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_CDR_BUILDING_BLDGVLTN as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APBLDGKEY), '0'), 38, 10) is null and 
                    src:APBLDGKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APBLDGVLTNKEY), '0'), 38, 10) is null and 
                    src:APBLDGVLTNKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BLDGAREA), '0'), 38, 10) is null and 
                    src:BLDGAREA is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CALCVLTN), '0'), 38, 10) is null and 
                    src:CALCVLTN is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MULT), '0'), 38, 10) is null and 
                    src:MULT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SUVLTNKEY), '0'), 38, 10) is null and 
                    src:SUVLTNKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)