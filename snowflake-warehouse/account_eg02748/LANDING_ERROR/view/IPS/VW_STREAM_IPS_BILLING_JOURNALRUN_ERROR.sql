CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_BILLING_JOURNALRUN_ERROR AS SELECT src, 'IPS_BILLING_JOURNALRUN' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AVGTRANPERMINUTE), '0'), 38, 10) is null and 
                    src:AVGTRANPERMINUTE is not null) THEN
                    'AVGTRANPERMINUTE contains a non-numeric value : \'' || AS_VARCHAR(src:AVGTRANPERMINUTE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMMITNUMBER), '0'), 38, 10) is null and 
                    src:COMMITNUMBER is not null) THEN
                    'COMMITNUMBER contains a non-numeric value : \'' || AS_VARCHAR(src:COMMITNUMBER) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ENDOFRUNDTTM), '1900-01-01')) is null and 
                    src:ENDOFRUNDTTM is not null) THEN
                    'ENDOFRUNDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ENDOFRUNDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:INITIATEDDTTM), '1900-01-01')) is null and 
                    src:INITIATEDDTTM is not null) THEN
                    'INITIATEDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:INITIATEDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:JOURNALRUNKEY), '0'), 38, 10) is null and 
                    src:JOURNALRUNKEY is not null) THEN
                    'JOURNALRUNKEY contains a non-numeric value : \'' || AS_VARCHAR(src:JOURNALRUNKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LASTINVOCATIONSTATUS), '0'), 38, 10) is null and 
                    src:LASTINVOCATIONSTATUS is not null) THEN
                    'LASTINVOCATIONSTATUS contains a non-numeric value : \'' || AS_VARCHAR(src:LASTINVOCATIONSTATUS) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMBEROFATPROCESSED), '0'), 38, 10) is null and 
                    src:NUMBEROFATPROCESSED is not null) THEN
                    'NUMBEROFATPROCESSED contains a non-numeric value : \'' || AS_VARCHAR(src:NUMBEROFATPROCESSED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMBEROFDTPROCESSED), '0'), 38, 10) is null and 
                    src:NUMBEROFDTPROCESSED is not null) THEN
                    'NUMBEROFDTPROCESSED contains a non-numeric value : \'' || AS_VARCHAR(src:NUMBEROFDTPROCESSED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SCHEDULEKEY), '0'), 38, 10) is null and 
                    src:SCHEDULEKEY is not null) THEN
                    'SCHEDULEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:SCHEDULEKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STARTOFRUNDTTM), '1900-01-01')) is null and 
                    src:STARTOFRUNDTTM is not null) THEN
                    'STARTOFRUNDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:STARTOFRUNDTTM) || '\'' WHEN 
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
                                    
                src:JOURNALRUNKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_JOURNALRUN as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AVGTRANPERMINUTE), '0'), 38, 10) is null and 
                    src:AVGTRANPERMINUTE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMMITNUMBER), '0'), 38, 10) is null and 
                    src:COMMITNUMBER is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ENDOFRUNDTTM), '1900-01-01')) is null and 
                    src:ENDOFRUNDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:INITIATEDDTTM), '1900-01-01')) is null and 
                    src:INITIATEDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:JOURNALRUNKEY), '0'), 38, 10) is null and 
                    src:JOURNALRUNKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LASTINVOCATIONSTATUS), '0'), 38, 10) is null and 
                    src:LASTINVOCATIONSTATUS is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMBEROFATPROCESSED), '0'), 38, 10) is null and 
                    src:NUMBEROFATPROCESSED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMBEROFDTPROCESSED), '0'), 38, 10) is null and 
                    src:NUMBEROFDTPROCESSED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SCHEDULEKEY), '0'), 38, 10) is null and 
                    src:SCHEDULEKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STARTOFRUNDTTM), '1900-01-01')) is null and 
                    src:STARTOFRUNDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)