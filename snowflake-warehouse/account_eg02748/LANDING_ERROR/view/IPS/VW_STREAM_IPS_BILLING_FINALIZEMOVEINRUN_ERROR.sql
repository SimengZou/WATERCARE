CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_BILLING_FINALIZEMOVEINRUN_ERROR AS SELECT src, 'IPS_BILLING_FINALIZEMOVEINRUN' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMMITNUMBER), '0'), 38, 10) is null and 
                    src:COMMITNUMBER is not null) THEN
                    'COMMITNUMBER contains a non-numeric value : \'' || AS_VARCHAR(src:COMMITNUMBER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FINALIZEMOVEINRUNKEY), '0'), 38, 10) is null and 
                    src:FINALIZEMOVEINRUNKEY is not null) THEN
                    'FINALIZEMOVEINRUNKEY contains a non-numeric value : \'' || AS_VARCHAR(src:FINALIZEMOVEINRUNKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LASTINVOCATIONSTATUS), '0'), 38, 10) is null and 
                    src:LASTINVOCATIONSTATUS is not null) THEN
                    'LASTINVOCATIONSTATUS contains a non-numeric value : \'' || AS_VARCHAR(src:LASTINVOCATIONSTATUS) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMBEROFEXCEPTIONS), '0'), 38, 10) is null and 
                    src:NUMBEROFEXCEPTIONS is not null) THEN
                    'NUMBEROFEXCEPTIONS contains a non-numeric value : \'' || AS_VARCHAR(src:NUMBEROFEXCEPTIONS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMBEROFFINALIZED), '0'), 38, 10) is null and 
                    src:NUMBEROFFINALIZED is not null) THEN
                    'NUMBEROFFINALIZED contains a non-numeric value : \'' || AS_VARCHAR(src:NUMBEROFFINALIZED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMBEROFPROCESSED), '0'), 38, 10) is null and 
                    src:NUMBEROFPROCESSED is not null) THEN
                    'NUMBEROFPROCESSED contains a non-numeric value : \'' || AS_VARCHAR(src:NUMBEROFPROCESSED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SCHEDULEKEY), '0'), 38, 10) is null and 
                    src:SCHEDULEKEY is not null) THEN
                    'SCHEDULEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:SCHEDULEKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STARTDTTM), '1900-01-01')) is null and 
                    src:STARTDTTM is not null) THEN
                    'STARTDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:STARTDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STOPDTTM), '1900-01-01')) is null and 
                    src:STOPDTTM is not null) THEN
                    'STOPDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:STOPDTTM) || '\'' WHEN 
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
                                    
                src:FINALIZEMOVEINRUNKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_FINALIZEMOVEINRUN as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMMITNUMBER), '0'), 38, 10) is null and 
                    src:COMMITNUMBER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FINALIZEMOVEINRUNKEY), '0'), 38, 10) is null and 
                    src:FINALIZEMOVEINRUNKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LASTINVOCATIONSTATUS), '0'), 38, 10) is null and 
                    src:LASTINVOCATIONSTATUS is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMBEROFEXCEPTIONS), '0'), 38, 10) is null and 
                    src:NUMBEROFEXCEPTIONS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMBEROFFINALIZED), '0'), 38, 10) is null and 
                    src:NUMBEROFFINALIZED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMBEROFPROCESSED), '0'), 38, 10) is null and 
                    src:NUMBEROFPROCESSED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SCHEDULEKEY), '0'), 38, 10) is null and 
                    src:SCHEDULEKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STARTDTTM), '1900-01-01')) is null and 
                    src:STARTDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STOPDTTM), '1900-01-01')) is null and 
                    src:STOPDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)