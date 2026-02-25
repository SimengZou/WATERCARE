CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_CDR_BUILDING_BLDGAPPLTYPE_ERROR AS SELECT src, 'IPS_CDR_BUILDING_BLDGAPPLTYPE' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCID), '0'), 38, 10) is null and 
                    src:ACCID is not null) THEN
                    'ACCID contains a non-numeric value : \'' || AS_VARCHAR(src:ACCID) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTTYPECONFIGURATION), '0'), 38, 10) is null and 
                    src:ACCOUNTTYPECONFIGURATION is not null) THEN
                    'ACCOUNTTYPECONFIGURATION contains a non-numeric value : \'' || AS_VARCHAR(src:ACCOUNTTYPECONFIGURATION) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APAPPLTYPECATKEY), '0'), 38, 10) is null and 
                    src:APAPPLTYPECATKEY is not null) THEN
                    'APAPPLTYPECATKEY contains a non-numeric value : \'' || AS_VARCHAR(src:APAPPLTYPECATKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APBLDGDEFNKEY), '0'), 38, 10) is null and 
                    src:APBLDGDEFNKEY is not null) THEN
                    'APBLDGDEFNKEY contains a non-numeric value : \'' || AS_VARCHAR(src:APBLDGDEFNKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLINGACCOUNTTYPE), '0'), 38, 10) is null and 
                    src:BILLINGACCOUNTTYPE is not null) THEN
                    'BILLINGACCOUNTTYPE contains a non-numeric value : \'' || AS_VARCHAR(src:BILLINGACCOUNTTYPE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLTYPEKEY), '0'), 38, 10) is null and 
                    src:BILLTYPEKEY is not null) THEN
                    'BILLTYPEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:BILLTYPEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DPLEVEL), '0'), 38, 10) is null and 
                    src:DPLEVEL is not null) THEN
                    'DPLEVEL contains a non-numeric value : \'' || AS_VARCHAR(src:DPLEVEL) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EFFDATE), '1900-01-01')) is null and 
                    src:EFFDATE is not null) THEN
                    'EFFDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:EFFDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EXPDATE), '1900-01-01')) is null and 
                    src:EXPDATE is not null) THEN
                    'EXPDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:EXPDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EXPDATEFRMLA), '0'), 38, 10) is null and 
                    src:EXPDATEFRMLA is not null) THEN
                    'EXPDATEFRMLA contains a non-numeric value : \'' || AS_VARCHAR(src:EXPDATEFRMLA) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PRIMARYSITETYPE), '0'), 38, 10) is null and 
                    src:PRIMARYSITETYPE is not null) THEN
                    'PRIMARYSITETYPE contains a non-numeric value : \'' || AS_VARCHAR(src:PRIMARYSITETYPE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RESPACCOUNTHOLDER), '0'), 38, 10) is null and 
                    src:RESPACCOUNTHOLDER is not null) THEN
                    'RESPACCOUNTHOLDER contains a non-numeric value : \'' || AS_VARCHAR(src:RESPACCOUNTHOLDER) || '\'' WHEN 
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
                                    
                src:APBLDGDEFNKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_CDR_BUILDING_BLDGAPPLTYPE as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCID), '0'), 38, 10) is null and 
                    src:ACCID is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTTYPECONFIGURATION), '0'), 38, 10) is null and 
                    src:ACCOUNTTYPECONFIGURATION is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APAPPLTYPECATKEY), '0'), 38, 10) is null and 
                    src:APAPPLTYPECATKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APBLDGDEFNKEY), '0'), 38, 10) is null and 
                    src:APBLDGDEFNKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLINGACCOUNTTYPE), '0'), 38, 10) is null and 
                    src:BILLINGACCOUNTTYPE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLTYPEKEY), '0'), 38, 10) is null and 
                    src:BILLTYPEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DPLEVEL), '0'), 38, 10) is null and 
                    src:DPLEVEL is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EFFDATE), '1900-01-01')) is null and 
                    src:EFFDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EXPDATE), '1900-01-01')) is null and 
                    src:EXPDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EXPDATEFRMLA), '0'), 38, 10) is null and 
                    src:EXPDATEFRMLA is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PRIMARYSITETYPE), '0'), 38, 10) is null and 
                    src:PRIMARYSITETYPE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RESPACCOUNTHOLDER), '0'), 38, 10) is null and 
                    src:RESPACCOUNTHOLDER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)