CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_CDR_BUILDING_BLDGPLANCOPY_ERROR AS SELECT src, 'IPS_CDR_BUILDING_BLDGPLANCOPY' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APBLDGKEY), '0'), 38, 10) is null and 
                    src:APBLDGKEY is not null) THEN
                    'APBLDGKEY contains a non-numeric value : \'' || AS_VARCHAR(src:APBLDGKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BLDGPLANCOPYKEY), '0'), 38, 10) is null and 
                    src:BLDGPLANCOPYKEY is not null) THEN
                    'BLDGPLANCOPYKEY contains a non-numeric value : \'' || AS_VARCHAR(src:BLDGPLANCOPYKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMMONPLANCOPY), '0'), 38, 10) is null and 
                    src:COMMONPLANCOPY is not null) THEN
                    'COMMONPLANCOPY contains a non-numeric value : \'' || AS_VARCHAR(src:COMMONPLANCOPY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COPYNO), '0'), 38, 10) is null and 
                    src:COPYNO is not null) THEN
                    'COPYNO contains a non-numeric value : \'' || AS_VARCHAR(src:COPYNO) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COPYSTATUS), '0'), 38, 10) is null and 
                    src:COPYSTATUS is not null) THEN
                    'COPYSTATUS contains a non-numeric value : \'' || AS_VARCHAR(src:COPYSTATUS) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:COPYSTATUSDTTM), '1900-01-01')) is null and 
                    src:COPYSTATUSDTTM is not null) THEN
                    'COPYSTATUSDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:COPYSTATUSDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CURRENTLOCDTTM), '1900-01-01')) is null and 
                    src:CURRENTLOCDTTM is not null) THEN
                    'CURRENTLOCDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:CURRENTLOCDTTM) || '\'' WHEN 
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
                                    
                src:BLDGPLANCOPYKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_CDR_BUILDING_BLDGPLANCOPY as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APBLDGKEY), '0'), 38, 10) is null and 
                    src:APBLDGKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BLDGPLANCOPYKEY), '0'), 38, 10) is null and 
                    src:BLDGPLANCOPYKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMMONPLANCOPY), '0'), 38, 10) is null and 
                    src:COMMONPLANCOPY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COPYNO), '0'), 38, 10) is null and 
                    src:COPYNO is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COPYSTATUS), '0'), 38, 10) is null and 
                    src:COPYSTATUS is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:COPYSTATUSDTTM), '1900-01-01')) is null and 
                    src:COPYSTATUSDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CURRENTLOCDTTM), '1900-01-01')) is null and 
                    src:CURRENTLOCDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)