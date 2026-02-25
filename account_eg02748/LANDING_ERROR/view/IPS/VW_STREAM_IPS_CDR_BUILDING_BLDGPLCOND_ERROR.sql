CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_CDR_BUILDING_BLDGPLCOND_ERROR AS SELECT src, 'IPS_CDR_BUILDING_BLDGPLCOND' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APBLDGKEY), '0'), 38, 10) is null and 
                    src:APBLDGKEY is not null) THEN
                    'APBLDGKEY contains a non-numeric value : \'' || AS_VARCHAR(src:APBLDGKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APBLDGPLCONDKEY), '0'), 38, 10) is null and 
                    src:APBLDGPLCONDKEY is not null) THEN
                    'APBLDGPLCONDKEY contains a non-numeric value : \'' || AS_VARCHAR(src:APBLDGPLCONDKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPLANSTATUSKEY), '0'), 38, 10) is null and 
                    src:APPLANSTATUSKEY is not null) THEN
                    'APPLANSTATUSKEY contains a non-numeric value : \'' || AS_VARCHAR(src:APPLANSTATUSKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AUTOSTATUSFRMKEY), '0'), 38, 10) is null and 
                    src:AUTOSTATUSFRMKEY is not null) THEN
                    'AUTOSTATUSFRMKEY contains a non-numeric value : \'' || AS_VARCHAR(src:AUTOSTATUSFRMKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BLDGAUTOSTATUSFRMKEY), '0'), 38, 10) is null and 
                    src:BLDGAUTOSTATUSFRMKEY is not null) THEN
                    'BLDGAUTOSTATUSFRMKEY contains a non-numeric value : \'' || AS_VARCHAR(src:BLDGAUTOSTATUSFRMKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CONDTYPE), '0'), 38, 10) is null and 
                    src:CONDTYPE is not null) THEN
                    'CONDTYPE contains a non-numeric value : \'' || AS_VARCHAR(src:CONDTYPE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DEFFERREDMILESTONE), '0'), 38, 10) is null and 
                    src:DEFFERREDMILESTONE is not null) THEN
                    'DEFFERREDMILESTONE contains a non-numeric value : \'' || AS_VARCHAR(src:DEFFERREDMILESTONE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EFFDATE), '1900-01-01')) is null and 
                    src:EFFDATE is not null) THEN
                    'EFFDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:EFFDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EXPDATE), '1900-01-01')) is null and 
                    src:EXPDATE is not null) THEN
                    'EXPDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:EXPDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INHERITRULEKEY), '0'), 38, 10) is null and 
                    src:INHERITRULEKEY is not null) THEN
                    'INHERITRULEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:INHERITRULEKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MILESTONEDATE), '1900-01-01')) is null and 
                    src:MILESTONEDATE is not null) THEN
                    'MILESTONEDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:MILESTONEDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MILESTONERULE), '0'), 38, 10) is null and 
                    src:MILESTONERULE is not null) THEN
                    'MILESTONERULE contains a non-numeric value : \'' || AS_VARCHAR(src:MILESTONERULE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORIGINAPKEY), '0'), 38, 10) is null and 
                    src:ORIGINAPKEY is not null) THEN
                    'ORIGINAPKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ORIGINAPKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORIGINCDRTYPE), '0'), 38, 10) is null and 
                    src:ORIGINCDRTYPE is not null) THEN
                    'ORIGINCDRTYPE contains a non-numeric value : \'' || AS_VARCHAR(src:ORIGINCDRTYPE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PARENTAPKEY), '0'), 38, 10) is null and 
                    src:PARENTAPKEY is not null) THEN
                    'PARENTAPKEY contains a non-numeric value : \'' || AS_VARCHAR(src:PARENTAPKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PARENTCDRTYPE), '0'), 38, 10) is null and 
                    src:PARENTCDRTYPE is not null) THEN
                    'PARENTCDRTYPE contains a non-numeric value : \'' || AS_VARCHAR(src:PARENTCDRTYPE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PLANAUTOSTATUSFRMKEY), '0'), 38, 10) is null and 
                    src:PLANAUTOSTATUSFRMKEY is not null) THEN
                    'PLANAUTOSTATUSFRMKEY contains a non-numeric value : \'' || AS_VARCHAR(src:PLANAUTOSTATUSFRMKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PLANCONDNO), '0'), 38, 10) is null and 
                    src:PLANCONDNO is not null) THEN
                    'PLANCONDNO contains a non-numeric value : \'' || AS_VARCHAR(src:PLANCONDNO) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PLCONDTYPE), '0'), 38, 10) is null and 
                    src:PLCONDTYPE is not null) THEN
                    'PLCONDTYPE contains a non-numeric value : \'' || AS_VARCHAR(src:PLCONDTYPE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PROJAUTOSTATUSFRMKEY), '0'), 38, 10) is null and 
                    src:PROJAUTOSTATUSFRMKEY is not null) THEN
                    'PROJAUTOSTATUSFRMKEY contains a non-numeric value : \'' || AS_VARCHAR(src:PROJAUTOSTATUSFRMKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STATUSDATE), '1900-01-01')) is null and 
                    src:STATUSDATE is not null) THEN
                    'STATUSDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:STATUSDATE) || '\'' WHEN 
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
                                    
                src:APBLDGPLCONDKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_CDR_BUILDING_BLDGPLCOND as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APBLDGKEY), '0'), 38, 10) is null and 
                    src:APBLDGKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APBLDGPLCONDKEY), '0'), 38, 10) is null and 
                    src:APBLDGPLCONDKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPLANSTATUSKEY), '0'), 38, 10) is null and 
                    src:APPLANSTATUSKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AUTOSTATUSFRMKEY), '0'), 38, 10) is null and 
                    src:AUTOSTATUSFRMKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BLDGAUTOSTATUSFRMKEY), '0'), 38, 10) is null and 
                    src:BLDGAUTOSTATUSFRMKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CONDTYPE), '0'), 38, 10) is null and 
                    src:CONDTYPE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DEFFERREDMILESTONE), '0'), 38, 10) is null and 
                    src:DEFFERREDMILESTONE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EFFDATE), '1900-01-01')) is null and 
                    src:EFFDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EXPDATE), '1900-01-01')) is null and 
                    src:EXPDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INHERITRULEKEY), '0'), 38, 10) is null and 
                    src:INHERITRULEKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MILESTONEDATE), '1900-01-01')) is null and 
                    src:MILESTONEDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MILESTONERULE), '0'), 38, 10) is null and 
                    src:MILESTONERULE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORIGINAPKEY), '0'), 38, 10) is null and 
                    src:ORIGINAPKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ORIGINCDRTYPE), '0'), 38, 10) is null and 
                    src:ORIGINCDRTYPE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PARENTAPKEY), '0'), 38, 10) is null and 
                    src:PARENTAPKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PARENTCDRTYPE), '0'), 38, 10) is null and 
                    src:PARENTCDRTYPE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PLANAUTOSTATUSFRMKEY), '0'), 38, 10) is null and 
                    src:PLANAUTOSTATUSFRMKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PLANCONDNO), '0'), 38, 10) is null and 
                    src:PLANCONDNO is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PLCONDTYPE), '0'), 38, 10) is null and 
                    src:PLCONDTYPE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PROJAUTOSTATUSFRMKEY), '0'), 38, 10) is null and 
                    src:PROJAUTOSTATUSFRMKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STATUSDATE), '1900-01-01')) is null and 
                    src:STATUSDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)