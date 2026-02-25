CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_CDR_PLANNING_PLANFEETYPE_ERROR AS SELECT src, 'IPS_CDR_PLANNING_PLANFEETYPE' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDCONDFRMLA), '0'), 38, 10) is null and 
                    src:ADDCONDFRMLA is not null) THEN
                    'ADDCONDFRMLA contains a non-numeric value : \'' || AS_VARCHAR(src:ADDCONDFRMLA) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDDEPPROCESSSTATEKEY), '0'), 38, 10) is null and 
                    src:ADDDEPPROCESSSTATEKEY is not null) THEN
                    'ADDDEPPROCESSSTATEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ADDDEPPROCESSSTATEKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPLANDEFNKEY), '0'), 38, 10) is null and 
                    src:APPLANDEFNKEY is not null) THEN
                    'APPLANDEFNKEY contains a non-numeric value : \'' || AS_VARCHAR(src:APPLANDEFNKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPLANFEETYPEKEY), '0'), 38, 10) is null and 
                    src:APPLANFEETYPEKEY is not null) THEN
                    'APPLANFEETYPEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:APPLANFEETYPEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPLANPROCESSSTATEKEY), '0'), 38, 10) is null and 
                    src:APPLANPROCESSSTATEKEY is not null) THEN
                    'APPLANPROCESSSTATEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:APPLANPROCESSSTATEKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:FEELIBTYPEUSEDFROMDT), '1900-01-01')) is null and 
                    src:FEELIBTYPEUSEDFROMDT is not null) THEN
                    'FEELIBTYPEUSEDFROMDT contains a non-timestamp value : \'' || AS_VARCHAR(src:FEELIBTYPEUSEDFROMDT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:FEELIBTYPEUSEDTODT), '1900-01-01')) is null and 
                    src:FEELIBTYPEUSEDTODT is not null) THEN
                    'FEELIBTYPEUSEDTODT contains a non-timestamp value : \'' || AS_VARCHAR(src:FEELIBTYPEUSEDTODT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FEETYPEKEY), '0'), 38, 10) is null and 
                    src:FEETYPEKEY is not null) THEN
                    'FEETYPEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:FEETYPEKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYFEEPROCESSSTATEKEY), '0'), 38, 10) is null and 
                    src:PAYFEEPROCESSSTATEKEY is not null) THEN
                    'PAYFEEPROCESSSTATEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:PAYFEEPROCESSSTATEKEY) || '\'' WHEN 
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
                                    
                src:APPLANFEETYPEKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_CDR_PLANNING_PLANFEETYPE as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDCONDFRMLA), '0'), 38, 10) is null and 
                    src:ADDCONDFRMLA is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDDEPPROCESSSTATEKEY), '0'), 38, 10) is null and 
                    src:ADDDEPPROCESSSTATEKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPLANDEFNKEY), '0'), 38, 10) is null and 
                    src:APPLANDEFNKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPLANFEETYPEKEY), '0'), 38, 10) is null and 
                    src:APPLANFEETYPEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPLANPROCESSSTATEKEY), '0'), 38, 10) is null and 
                    src:APPLANPROCESSSTATEKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:FEELIBTYPEUSEDFROMDT), '1900-01-01')) is null and 
                    src:FEELIBTYPEUSEDFROMDT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:FEELIBTYPEUSEDTODT), '1900-01-01')) is null and 
                    src:FEELIBTYPEUSEDTODT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FEETYPEKEY), '0'), 38, 10) is null and 
                    src:FEETYPEKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYFEEPROCESSSTATEKEY), '0'), 38, 10) is null and 
                    src:PAYFEEPROCESSSTATEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)