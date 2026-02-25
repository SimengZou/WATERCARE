CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_CDR_PLANNING_PLANPLCONDINHERITRULE_ERROR AS SELECT src, 'IPS_CDR_PLANNING_PLANPLCONDINHERITRULE' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPLANPLCONDINHERITRULEKEY), '0'), 38, 10) is null and 
                    src:APPLANPLCONDINHERITRULEKEY is not null) THEN
                    'APPLANPLCONDINHERITRULEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:APPLANPLCONDINHERITRULEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BLDGINHERITFRMLAKEY), '0'), 38, 10) is null and 
                    src:BLDGINHERITFRMLAKEY is not null) THEN
                    'BLDGINHERITFRMLAKEY contains a non-numeric value : \'' || AS_VARCHAR(src:BLDGINHERITFRMLAKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CDSYSTEMAPPLTYPE), '0'), 38, 10) is null and 
                    src:CDSYSTEMAPPLTYPE is not null) THEN
                    'CDSYSTEMAPPLTYPE contains a non-numeric value : \'' || AS_VARCHAR(src:CDSYSTEMAPPLTYPE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INHERITFRMLA), '0'), 38, 10) is null and 
                    src:INHERITFRMLA is not null) THEN
                    'INHERITFRMLA contains a non-numeric value : \'' || AS_VARCHAR(src:INHERITFRMLA) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INHERITRULETYPE), '0'), 38, 10) is null and 
                    src:INHERITRULETYPE is not null) THEN
                    'INHERITRULETYPE contains a non-numeric value : \'' || AS_VARCHAR(src:INHERITRULETYPE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PLANINHERITFRMLAKEY), '0'), 38, 10) is null and 
                    src:PLANINHERITFRMLAKEY is not null) THEN
                    'PLANINHERITFRMLAKEY contains a non-numeric value : \'' || AS_VARCHAR(src:PLANINHERITFRMLAKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PROJINHERITFRMLAKEY), '0'), 38, 10) is null and 
                    src:PROJINHERITFRMLAKEY is not null) THEN
                    'PROJINHERITFRMLAKEY contains a non-numeric value : \'' || AS_VARCHAR(src:PROJINHERITFRMLAKEY) || '\'' WHEN 
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
                                    
                src:APPLANPLCONDINHERITRULEKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_CDR_PLANNING_PLANPLCONDINHERITRULE as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPLANPLCONDINHERITRULEKEY), '0'), 38, 10) is null and 
                    src:APPLANPLCONDINHERITRULEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BLDGINHERITFRMLAKEY), '0'), 38, 10) is null and 
                    src:BLDGINHERITFRMLAKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CDSYSTEMAPPLTYPE), '0'), 38, 10) is null and 
                    src:CDSYSTEMAPPLTYPE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INHERITFRMLA), '0'), 38, 10) is null and 
                    src:INHERITFRMLA is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INHERITRULETYPE), '0'), 38, 10) is null and 
                    src:INHERITRULETYPE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PLANINHERITFRMLAKEY), '0'), 38, 10) is null and 
                    src:PLANINHERITFRMLAKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PROJINHERITFRMLAKEY), '0'), 38, 10) is null and 
                    src:PROJINHERITFRMLAKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)