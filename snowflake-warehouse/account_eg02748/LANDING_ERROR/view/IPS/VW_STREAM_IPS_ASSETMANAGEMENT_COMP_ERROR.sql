CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_ASSETMANAGEMENT_COMP_ERROR AS SELECT src, 'IPS_ASSETMANAGEMENT_COMP' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDRKEY), '0'), 38, 10) is null and 
                    src:ADDRKEY is not null) THEN
                    'ADDRKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ADDRKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BLDGFLOOR), '0'), 38, 10) is null and 
                    src:BLDGFLOOR is not null) THEN
                    'BLDGFLOOR contains a non-numeric value : \'' || AS_VARCHAR(src:BLDGFLOOR) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BLDGROOM), '0'), 38, 10) is null and 
                    src:BLDGROOM is not null) THEN
                    'BLDGROOM contains a non-numeric value : \'' || AS_VARCHAR(src:BLDGROOM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPKEY), '0'), 38, 10) is null and 
                    src:COMPKEY is not null) THEN
                    'COMPKEY contains a non-numeric value : \'' || AS_VARCHAR(src:COMPKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPLEXKEY), '0'), 38, 10) is null and 
                    src:COMPLEXKEY is not null) THEN
                    'COMPLEXKEY contains a non-numeric value : \'' || AS_VARCHAR(src:COMPLEXKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPTYPE), '0'), 38, 10) is null and 
                    src:COMPTYPE is not null) THEN
                    'COMPTYPE contains a non-numeric value : \'' || AS_VARCHAR(src:COMPTYPE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EXPDATE), '1900-01-01')) is null and 
                    src:EXPDATE is not null) THEN
                    'EXPDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:EXPDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INTKEY), '0'), 38, 10) is null and 
                    src:INTKEY is not null) THEN
                    'INTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:INTKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MAINCOMP1), '0'), 38, 10) is null and 
                    src:MAINCOMP1 is not null) THEN
                    'MAINCOMP1 contains a non-numeric value : \'' || AS_VARCHAR(src:MAINCOMP1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MAINCOMP2), '0'), 38, 10) is null and 
                    src:MAINCOMP2 is not null) THEN
                    'MAINCOMP2 contains a non-numeric value : \'' || AS_VARCHAR(src:MAINCOMP2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MAINKEY), '0'), 38, 10) is null and 
                    src:MAINKEY is not null) THEN
                    'MAINKEY contains a non-numeric value : \'' || AS_VARCHAR(src:MAINKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MAINKEY1), '0'), 38, 10) is null and 
                    src:MAINKEY1 is not null) THEN
                    'MAINKEY1 contains a non-numeric value : \'' || AS_VARCHAR(src:MAINKEY1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MAINKEY2), '0'), 38, 10) is null and 
                    src:MAINKEY2 is not null) THEN
                    'MAINKEY2 contains a non-numeric value : \'' || AS_VARCHAR(src:MAINKEY2) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PRCLKEY), '0'), 38, 10) is null and 
                    src:PRCLKEY is not null) THEN
                    'PRCLKEY contains a non-numeric value : \'' || AS_VARCHAR(src:PRCLKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RSEGKEY), '0'), 38, 10) is null and 
                    src:RSEGKEY is not null) THEN
                    'RSEGKEY contains a non-numeric value : \'' || AS_VARCHAR(src:RSEGKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SEGKEY), '0'), 38, 10) is null and 
                    src:SEGKEY is not null) THEN
                    'SEGKEY contains a non-numeric value : \'' || AS_VARCHAR(src:SEGKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SITEKEY), '0'), 38, 10) is null and 
                    src:SITEKEY is not null) THEN
                    'SITEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:SITEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SLKEY), '0'), 38, 10) is null and 
                    src:SLKEY is not null) THEN
                    'SLKEY contains a non-numeric value : \'' || AS_VARCHAR(src:SLKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USGAREAKEY), '0'), 38, 10) is null and 
                    src:USGAREAKEY is not null) THEN
                    'USGAREAKEY contains a non-numeric value : \'' || AS_VARCHAR(src:USGAREAKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) THEN
                    'VARIATION_ID contains a non-numeric value : \'' || AS_VARCHAR(src:VARIATION_ID) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XINGKEY), '0'), 38, 10) is null and 
                    src:XINGKEY is not null) THEN
                    'XINGKEY contains a non-numeric value : \'' || AS_VARCHAR(src:XINGKEY) || '\'' WHEN 
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
                                    
                src:COMPKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_ASSETMANAGEMENT_COMP as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDRKEY), '0'), 38, 10) is null and 
                    src:ADDRKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BLDGFLOOR), '0'), 38, 10) is null and 
                    src:BLDGFLOOR is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BLDGROOM), '0'), 38, 10) is null and 
                    src:BLDGROOM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPKEY), '0'), 38, 10) is null and 
                    src:COMPKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPLEXKEY), '0'), 38, 10) is null and 
                    src:COMPLEXKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPTYPE), '0'), 38, 10) is null and 
                    src:COMPTYPE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EXPDATE), '1900-01-01')) is null and 
                    src:EXPDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INTKEY), '0'), 38, 10) is null and 
                    src:INTKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MAINCOMP1), '0'), 38, 10) is null and 
                    src:MAINCOMP1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MAINCOMP2), '0'), 38, 10) is null and 
                    src:MAINCOMP2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MAINKEY), '0'), 38, 10) is null and 
                    src:MAINKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MAINKEY1), '0'), 38, 10) is null and 
                    src:MAINKEY1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MAINKEY2), '0'), 38, 10) is null and 
                    src:MAINKEY2 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PRCLKEY), '0'), 38, 10) is null and 
                    src:PRCLKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RSEGKEY), '0'), 38, 10) is null and 
                    src:RSEGKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SEGKEY), '0'), 38, 10) is null and 
                    src:SEGKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SITEKEY), '0'), 38, 10) is null and 
                    src:SITEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SLKEY), '0'), 38, 10) is null and 
                    src:SLKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USGAREAKEY), '0'), 38, 10) is null and 
                    src:USGAREAKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XINGKEY), '0'), 38, 10) is null and 
                    src:XINGKEY is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)