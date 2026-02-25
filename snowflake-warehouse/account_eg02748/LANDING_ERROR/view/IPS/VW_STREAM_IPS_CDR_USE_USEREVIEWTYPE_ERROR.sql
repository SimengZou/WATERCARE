CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_CDR_USE_USEREVIEWTYPE_ERROR AS SELECT src, 'IPS_CDR_USE_USEREVIEWTYPE' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDCONDFRMLA), '0'), 38, 10) is null and 
                    src:ADDCONDFRMLA is not null) THEN
                    'ADDCONDFRMLA contains a non-numeric value : \'' || AS_VARCHAR(src:ADDCONDFRMLA) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APUSEDEFNKEY), '0'), 38, 10) is null and 
                    src:APUSEDEFNKEY is not null) THEN
                    'APUSEDEFNKEY contains a non-numeric value : \'' || AS_VARCHAR(src:APUSEDEFNKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APUSEPROCESSSTATEKEY), '0'), 38, 10) is null and 
                    src:APUSEPROCESSSTATEKEY is not null) THEN
                    'APUSEPROCESSSTATEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:APUSEPROCESSSTATEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APUSEREVIEWTYPEKEY), '0'), 38, 10) is null and 
                    src:APUSEREVIEWTYPEKEY is not null) THEN
                    'APUSEREVIEWTYPEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:APUSEREVIEWTYPEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ASSGNGRPFRMLA), '0'), 38, 10) is null and 
                    src:ASSGNGRPFRMLA is not null) THEN
                    'ASSGNGRPFRMLA contains a non-numeric value : \'' || AS_VARCHAR(src:ASSGNGRPFRMLA) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EFFDATE), '1900-01-01')) is null and 
                    src:EFFDATE is not null) THEN
                    'EFFDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:EFFDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EXPDATE), '1900-01-01')) is null and 
                    src:EXPDATE is not null) THEN
                    'EXPDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:EXPDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PLANCOPYNO), '0'), 38, 10) is null and 
                    src:PLANCOPYNO is not null) THEN
                    'PLANCOPYNO contains a non-numeric value : \'' || AS_VARCHAR(src:PLANCOPYNO) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SUSPDAYS), '0'), 38, 10) is null and 
                    src:SUSPDAYS is not null) THEN
                    'SUSPDAYS contains a non-numeric value : \'' || AS_VARCHAR(src:SUSPDAYS) || '\'' WHEN 
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
                                    
                src:APUSEREVIEWTYPEKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_CDR_USE_USEREVIEWTYPE as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDCONDFRMLA), '0'), 38, 10) is null and 
                    src:ADDCONDFRMLA is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APUSEDEFNKEY), '0'), 38, 10) is null and 
                    src:APUSEDEFNKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APUSEPROCESSSTATEKEY), '0'), 38, 10) is null and 
                    src:APUSEPROCESSSTATEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APUSEREVIEWTYPEKEY), '0'), 38, 10) is null and 
                    src:APUSEREVIEWTYPEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ASSGNGRPFRMLA), '0'), 38, 10) is null and 
                    src:ASSGNGRPFRMLA is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EFFDATE), '1900-01-01')) is null and 
                    src:EFFDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EXPDATE), '1900-01-01')) is null and 
                    src:EXPDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PLANCOPYNO), '0'), 38, 10) is null and 
                    src:PLANCOPYNO is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SUSPDAYS), '0'), 38, 10) is null and 
                    src:SUSPDAYS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)