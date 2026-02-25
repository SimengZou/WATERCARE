CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_CDR_PLANNING_PLANCONSTRAINTS_ERROR AS SELECT src, 'IPS_CDR_PLANNING_PLANCONSTRAINTS' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APTYPEKEY), '0'), 38, 10) is null and 
                    src:APTYPEKEY is not null) THEN
                    'APTYPEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:APTYPEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CONSTRAINTCONTROLTYPE), '0'), 38, 10) is null and 
                    src:CONSTRAINTCONTROLTYPE is not null) THEN
                    'CONSTRAINTCONTROLTYPE contains a non-numeric value : \'' || AS_VARCHAR(src:CONSTRAINTCONTROLTYPE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MILESTONE), '0'), 38, 10) is null and 
                    src:MILESTONE is not null) THEN
                    'MILESTONE contains a non-numeric value : \'' || AS_VARCHAR(src:MILESTONE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PLANCONSTRAINTSKEY), '0'), 38, 10) is null and 
                    src:PLANCONSTRAINTSKEY is not null) THEN
                    'PLANCONSTRAINTSKEY contains a non-numeric value : \'' || AS_VARCHAR(src:PLANCONSTRAINTSKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SETTING), '0'), 38, 10) is null and 
                    src:SETTING is not null) THEN
                    'SETTING contains a non-numeric value : \'' || AS_VARCHAR(src:SETTING) || '\'' WHEN 
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
                                    
                src:PLANCONSTRAINTSKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_CDR_PLANNING_PLANCONSTRAINTS as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APTYPEKEY), '0'), 38, 10) is null and 
                    src:APTYPEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CONSTRAINTCONTROLTYPE), '0'), 38, 10) is null and 
                    src:CONSTRAINTCONTROLTYPE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MILESTONE), '0'), 38, 10) is null and 
                    src:MILESTONE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PLANCONSTRAINTSKEY), '0'), 38, 10) is null and 
                    src:PLANCONSTRAINTSKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SETTING), '0'), 38, 10) is null and 
                    src:SETTING is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)