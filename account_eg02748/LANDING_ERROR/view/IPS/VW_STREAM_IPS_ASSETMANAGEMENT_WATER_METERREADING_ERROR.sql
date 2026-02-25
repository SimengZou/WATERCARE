CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_ASSETMANAGEMENT_WATER_METERREADING_ERROR AS SELECT src, 'IPS_ASSETMANAGEMENT_WATER_METERREADING' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BLUSAGE), '0'), 38, 10) is null and 
                    src:BLUSAGE is not null) THEN
                    'BLUSAGE contains a non-numeric value : \'' || AS_VARCHAR(src:BLUSAGE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BLUSAGECUBICFT), '0'), 38, 10) is null and 
                    src:BLUSAGECUBICFT is not null) THEN
                    'BLUSAGECUBICFT contains a non-numeric value : \'' || AS_VARCHAR(src:BLUSAGECUBICFT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPKEY), '0'), 38, 10) is null and 
                    src:COMPKEY is not null) THEN
                    'COMPKEY contains a non-numeric value : \'' || AS_VARCHAR(src:COMPKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CUSTPROB), '0'), 38, 10) is null and 
                    src:CUSTPROB is not null) THEN
                    'CUSTPROB contains a non-numeric value : \'' || AS_VARCHAR(src:CUSTPROB) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:GRPPROJ), '0'), 38, 10) is null and 
                    src:GRPPROJ is not null) THEN
                    'GRPPROJ contains a non-numeric value : \'' || AS_VARCHAR(src:GRPPROJ) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HIGHBLUSAGE), '0'), 38, 10) is null and 
                    src:HIGHBLUSAGE is not null) THEN
                    'HIGHBLUSAGE contains a non-numeric value : \'' || AS_VARCHAR(src:HIGHBLUSAGE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HIGHBLUSAGEINCUBICFEET), '0'), 38, 10) is null and 
                    src:HIGHBLUSAGEINCUBICFEET is not null) THEN
                    'HIGHBLUSAGEINCUBICFEET contains a non-numeric value : \'' || AS_VARCHAR(src:HIGHBLUSAGEINCUBICFEET) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HIGHREADING), '0'), 38, 10) is null and 
                    src:HIGHREADING is not null) THEN
                    'HIGHREADING contains a non-numeric value : \'' || AS_VARCHAR(src:HIGHREADING) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HIGHUSAGE), '0'), 38, 10) is null and 
                    src:HIGHUSAGE is not null) THEN
                    'HIGHUSAGE contains a non-numeric value : \'' || AS_VARCHAR(src:HIGHUSAGE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HISTKEY), '0'), 38, 10) is null and 
                    src:HISTKEY is not null) THEN
                    'HISTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:HISTKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INSPKEY), '0'), 38, 10) is null and 
                    src:INSPKEY is not null) THEN
                    'INSPKEY contains a non-numeric value : \'' || AS_VARCHAR(src:INSPKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PROBBCODEWO), '0'), 38, 10) is null and 
                    src:PROBBCODEWO is not null) THEN
                    'PROBBCODEWO contains a non-numeric value : \'' || AS_VARCHAR(src:PROBBCODEWO) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PROBCODESRVREQNO), '0'), 38, 10) is null and 
                    src:PROBCODESRVREQNO is not null) THEN
                    'PROBCODESRVREQNO contains a non-numeric value : \'' || AS_VARCHAR(src:PROBCODESRVREQNO) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RDRCODESRVREQNO), '0'), 38, 10) is null and 
                    src:RDRCODESRVREQNO is not null) THEN
                    'RDRCODESRVREQNO contains a non-numeric value : \'' || AS_VARCHAR(src:RDRCODESRVREQNO) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:READDTTM), '1900-01-01')) is null and 
                    src:READDTTM is not null) THEN
                    'READDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:READDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:READERCODEWO), '0'), 38, 10) is null and 
                    src:READERCODEWO is not null) THEN
                    'READERCODEWO contains a non-numeric value : \'' || AS_VARCHAR(src:READERCODEWO) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:READING), '0'), 38, 10) is null and 
                    src:READING is not null) THEN
                    'READING contains a non-numeric value : \'' || AS_VARCHAR(src:READING) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:READINGKEY), '0'), 38, 10) is null and 
                    src:READINGKEY is not null) THEN
                    'READINGKEY contains a non-numeric value : \'' || AS_VARCHAR(src:READINGKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USAGE), '0'), 38, 10) is null and 
                    src:USAGE is not null) THEN
                    'USAGE contains a non-numeric value : \'' || AS_VARCHAR(src:USAGE) || '\'' WHEN 
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
                                    
                src:READINGKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_ASSETMANAGEMENT_WATER_METERREADING as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BLUSAGE), '0'), 38, 10) is null and 
                    src:BLUSAGE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BLUSAGECUBICFT), '0'), 38, 10) is null and 
                    src:BLUSAGECUBICFT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPKEY), '0'), 38, 10) is null and 
                    src:COMPKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CUSTPROB), '0'), 38, 10) is null and 
                    src:CUSTPROB is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:GRPPROJ), '0'), 38, 10) is null and 
                    src:GRPPROJ is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HIGHBLUSAGE), '0'), 38, 10) is null and 
                    src:HIGHBLUSAGE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HIGHBLUSAGEINCUBICFEET), '0'), 38, 10) is null and 
                    src:HIGHBLUSAGEINCUBICFEET is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HIGHREADING), '0'), 38, 10) is null and 
                    src:HIGHREADING is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HIGHUSAGE), '0'), 38, 10) is null and 
                    src:HIGHUSAGE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HISTKEY), '0'), 38, 10) is null and 
                    src:HISTKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INSPKEY), '0'), 38, 10) is null and 
                    src:INSPKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PROBBCODEWO), '0'), 38, 10) is null and 
                    src:PROBBCODEWO is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PROBCODESRVREQNO), '0'), 38, 10) is null and 
                    src:PROBCODESRVREQNO is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RDRCODESRVREQNO), '0'), 38, 10) is null and 
                    src:RDRCODESRVREQNO is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:READDTTM), '1900-01-01')) is null and 
                    src:READDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:READERCODEWO), '0'), 38, 10) is null and 
                    src:READERCODEWO is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:READING), '0'), 38, 10) is null and 
                    src:READING is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:READINGKEY), '0'), 38, 10) is null and 
                    src:READINGKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USAGE), '0'), 38, 10) is null and 
                    src:USAGE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)