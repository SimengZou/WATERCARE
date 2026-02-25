CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_CDR_PLANNING_PLANPLCONDTYPE_ERROR AS SELECT src, 'IPS_CDR_PLANNING_PLANPLCONDTYPE' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPLANCONDSTATUSKEY), '0'), 38, 10) is null and 
                    src:APPLANCONDSTATUSKEY is not null) THEN
                    'APPLANCONDSTATUSKEY contains a non-numeric value : \'' || AS_VARCHAR(src:APPLANCONDSTATUSKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPLANPLCONDINHERITKEY), '0'), 38, 10) is null and 
                    src:APPLANPLCONDINHERITKEY is not null) THEN
                    'APPLANPLCONDINHERITKEY contains a non-numeric value : \'' || AS_VARCHAR(src:APPLANPLCONDINHERITKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPLANPLCONDMILESTONEKEY), '0'), 38, 10) is null and 
                    src:APPLANPLCONDMILESTONEKEY is not null) THEN
                    'APPLANPLCONDMILESTONEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:APPLANPLCONDMILESTONEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPLANPLCONDTYPECATKEY), '0'), 38, 10) is null and 
                    src:APPLANPLCONDTYPECATKEY is not null) THEN
                    'APPLANPLCONDTYPECATKEY contains a non-numeric value : \'' || AS_VARCHAR(src:APPLANPLCONDTYPECATKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPLANPLCONDTYPEKEY), '0'), 38, 10) is null and 
                    src:APPLANPLCONDTYPEKEY is not null) THEN
                    'APPLANPLCONDTYPEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:APPLANPLCONDTYPEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AUTOSTATUSFRMLKEY), '0'), 38, 10) is null and 
                    src:AUTOSTATUSFRMLKEY is not null) THEN
                    'AUTOSTATUSFRMLKEY contains a non-numeric value : \'' || AS_VARCHAR(src:AUTOSTATUSFRMLKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BLDGAUTOSTATUSFRMLKEY), '0'), 38, 10) is null and 
                    src:BLDGAUTOSTATUSFRMLKEY is not null) THEN
                    'BLDGAUTOSTATUSFRMLKEY contains a non-numeric value : \'' || AS_VARCHAR(src:BLDGAUTOSTATUSFRMLKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CONDTYPE), '0'), 38, 10) is null and 
                    src:CONDTYPE is not null) THEN
                    'CONDTYPE contains a non-numeric value : \'' || AS_VARCHAR(src:CONDTYPE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EFFDATE), '1900-01-01')) is null and 
                    src:EFFDATE is not null) THEN
                    'EFFDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:EFFDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EXPDATE), '1900-01-01')) is null and 
                    src:EXPDATE is not null) THEN
                    'EXPDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:EXPDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PLANAUTOSTATUSFRMLKEY), '0'), 38, 10) is null and 
                    src:PLANAUTOSTATUSFRMLKEY is not null) THEN
                    'PLANAUTOSTATUSFRMLKEY contains a non-numeric value : \'' || AS_VARCHAR(src:PLANAUTOSTATUSFRMLKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PROJAUTOSTATUSFRMLKEY), '0'), 38, 10) is null and 
                    src:PROJAUTOSTATUSFRMLKEY is not null) THEN
                    'PROJAUTOSTATUSFRMLKEY contains a non-numeric value : \'' || AS_VARCHAR(src:PROJAUTOSTATUSFRMLKEY) || '\'' WHEN 
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
                                    
                src:APPLANPLCONDTYPEKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_CDR_PLANNING_PLANPLCONDTYPE as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPLANCONDSTATUSKEY), '0'), 38, 10) is null and 
                    src:APPLANCONDSTATUSKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPLANPLCONDINHERITKEY), '0'), 38, 10) is null and 
                    src:APPLANPLCONDINHERITKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPLANPLCONDMILESTONEKEY), '0'), 38, 10) is null and 
                    src:APPLANPLCONDMILESTONEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPLANPLCONDTYPECATKEY), '0'), 38, 10) is null and 
                    src:APPLANPLCONDTYPECATKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPLANPLCONDTYPEKEY), '0'), 38, 10) is null and 
                    src:APPLANPLCONDTYPEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AUTOSTATUSFRMLKEY), '0'), 38, 10) is null and 
                    src:AUTOSTATUSFRMLKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BLDGAUTOSTATUSFRMLKEY), '0'), 38, 10) is null and 
                    src:BLDGAUTOSTATUSFRMLKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CONDTYPE), '0'), 38, 10) is null and 
                    src:CONDTYPE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EFFDATE), '1900-01-01')) is null and 
                    src:EFFDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EXPDATE), '1900-01-01')) is null and 
                    src:EXPDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PLANAUTOSTATUSFRMLKEY), '0'), 38, 10) is null and 
                    src:PLANAUTOSTATUSFRMLKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PROJAUTOSTATUSFRMLKEY), '0'), 38, 10) is null and 
                    src:PROJAUTOSTATUSFRMLKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)