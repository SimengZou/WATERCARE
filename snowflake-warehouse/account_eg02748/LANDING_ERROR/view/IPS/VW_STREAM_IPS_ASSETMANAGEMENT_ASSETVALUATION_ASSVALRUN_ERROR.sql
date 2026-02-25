CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_ASSETMANAGEMENT_ASSETVALUATION_ASSVALRUN_ERROR AS SELECT src, 'IPS_ASSETMANAGEMENT_ASSETVALUATION_ASSVALRUN' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ASSVALGRPKEY), '0'), 38, 10) is null and 
                    src:ASSVALGRPKEY is not null) THEN
                    'ASSVALGRPKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ASSVALGRPKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ASSVALRUNKEY), '0'), 38, 10) is null and 
                    src:ASSVALRUNKEY is not null) THEN
                    'ASSVALRUNKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ASSVALRUNKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:INITIALBATCHDTTM), '1900-01-01')) is null and 
                    src:INITIALBATCHDTTM is not null) THEN
                    'INITIALBATCHDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:INITIALBATCHDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:INITIALRUNCOMPLETEDTTM), '1900-01-01')) is null and 
                    src:INITIALRUNCOMPLETEDTTM is not null) THEN
                    'INITIALRUNCOMPLETEDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:INITIALRUNCOMPLETEDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:INITIALRUNDDTTM), '1900-01-01')) is null and 
                    src:INITIALRUNDDTTM is not null) THEN
                    'INITIALRUNDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:INITIALRUNDDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:JOURNALCOMMITDTTM), '1900-01-01')) is null and 
                    src:JOURNALCOMMITDTTM is not null) THEN
                    'JOURNALCOMMITDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:JOURNALCOMMITDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:JOURNALDTTM), '1900-01-01')) is null and 
                    src:JOURNALDTTM is not null) THEN
                    'JOURNALDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:JOURNALDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:JOURNALREVERSALDTTM), '1900-01-01')) is null and 
                    src:JOURNALREVERSALDTTM is not null) THEN
                    'JOURNALREVERSALDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:JOURNALREVERSALDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMDEPRE), '0'), 38, 10) is null and 
                    src:NUMDEPRE is not null) THEN
                    'NUMDEPRE contains a non-numeric value : \'' || AS_VARCHAR(src:NUMDEPRE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMREVAL), '0'), 38, 10) is null and 
                    src:NUMREVAL is not null) THEN
                    'NUMREVAL contains a non-numeric value : \'' || AS_VARCHAR(src:NUMREVAL) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMVAL), '0'), 38, 10) is null and 
                    src:NUMVAL is not null) THEN
                    'NUMVAL contains a non-numeric value : \'' || AS_VARCHAR(src:NUMVAL) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RUNTYPE), '0'), 38, 10) is null and 
                    src:RUNTYPE is not null) THEN
                    'RUNTYPE contains a non-numeric value : \'' || AS_VARCHAR(src:RUNTYPE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SCHEDULEDDATE), '1900-01-01')) is null and 
                    src:SCHEDULEDDATE is not null) THEN
                    'SCHEDULEDDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:SCHEDULEDDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STAGE), '0'), 38, 10) is null and 
                    src:STAGE is not null) THEN
                    'STAGE contains a non-numeric value : \'' || AS_VARCHAR(src:STAGE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VALDTTM), '1900-01-01')) is null and 
                    src:VALDTTM is not null) THEN
                    'VALDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:VALDTTM) || '\'' WHEN 
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
                                    
                src:ASSVALRUNKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_ASSETMANAGEMENT_ASSETVALUATION_ASSVALRUN as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ASSVALGRPKEY), '0'), 38, 10) is null and 
                    src:ASSVALGRPKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ASSVALRUNKEY), '0'), 38, 10) is null and 
                    src:ASSVALRUNKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:INITIALBATCHDTTM), '1900-01-01')) is null and 
                    src:INITIALBATCHDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:INITIALRUNCOMPLETEDTTM), '1900-01-01')) is null and 
                    src:INITIALRUNCOMPLETEDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:INITIALRUNDDTTM), '1900-01-01')) is null and 
                    src:INITIALRUNDDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:JOURNALCOMMITDTTM), '1900-01-01')) is null and 
                    src:JOURNALCOMMITDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:JOURNALDTTM), '1900-01-01')) is null and 
                    src:JOURNALDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:JOURNALREVERSALDTTM), '1900-01-01')) is null and 
                    src:JOURNALREVERSALDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMDEPRE), '0'), 38, 10) is null and 
                    src:NUMDEPRE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMREVAL), '0'), 38, 10) is null and 
                    src:NUMREVAL is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMVAL), '0'), 38, 10) is null and 
                    src:NUMVAL is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RUNTYPE), '0'), 38, 10) is null and 
                    src:RUNTYPE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SCHEDULEDDATE), '1900-01-01')) is null and 
                    src:SCHEDULEDDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STAGE), '0'), 38, 10) is null and 
                    src:STAGE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VALDTTM), '1900-01-01')) is null and 
                    src:VALDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)