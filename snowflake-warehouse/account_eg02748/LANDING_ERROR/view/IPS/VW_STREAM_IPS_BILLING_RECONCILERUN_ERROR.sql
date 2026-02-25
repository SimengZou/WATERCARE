CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_BILLING_RECONCILERUN_ERROR AS SELECT src, 'IPS_BILLING_RECONCILERUN' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTFROM), '0'), 38, 10) is null and 
                    src:ACCOUNTFROM is not null) THEN
                    'ACCOUNTFROM contains a non-numeric value : \'' || AS_VARCHAR(src:ACCOUNTFROM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTSRECONCILED), '0'), 38, 10) is null and 
                    src:ACCOUNTSRECONCILED is not null) THEN
                    'ACCOUNTSRECONCILED contains a non-numeric value : \'' || AS_VARCHAR(src:ACCOUNTSRECONCILED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTTO), '0'), 38, 10) is null and 
                    src:ACCOUNTTO is not null) THEN
                    'ACCOUNTTO contains a non-numeric value : \'' || AS_VARCHAR(src:ACCOUNTTO) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMMITNUMBER), '0'), 38, 10) is null and 
                    src:COMMITNUMBER is not null) THEN
                    'COMMITNUMBER contains a non-numeric value : \'' || AS_VARCHAR(src:COMMITNUMBER) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:COMPLETEDDTTM), '1900-01-01')) is null and 
                    src:COMPLETEDDTTM is not null) THEN
                    'COMPLETEDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:COMPLETEDDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:INITIATEDDTTM), '1900-01-01')) is null and 
                    src:INITIATEDDTTM is not null) THEN
                    'INITIATEDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:INITIATEDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LASTINVOCATIONSTATUS), '0'), 38, 10) is null and 
                    src:LASTINVOCATIONSTATUS is not null) THEN
                    'LASTINVOCATIONSTATUS contains a non-numeric value : \'' || AS_VARCHAR(src:LASTINVOCATIONSTATUS) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RECONCILERUNKEY), '0'), 38, 10) is null and 
                    src:RECONCILERUNKEY is not null) THEN
                    'RECONCILERUNKEY contains a non-numeric value : \'' || AS_VARCHAR(src:RECONCILERUNKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SCHEDULEKEY), '0'), 38, 10) is null and 
                    src:SCHEDULEKEY is not null) THEN
                    'SCHEDULEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:SCHEDULEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TOTALNUMBEROFACCOUNTS), '0'), 38, 10) is null and 
                    src:TOTALNUMBEROFACCOUNTS is not null) THEN
                    'TOTALNUMBEROFACCOUNTS contains a non-numeric value : \'' || AS_VARCHAR(src:TOTALNUMBEROFACCOUNTS) || '\'' WHEN 
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
                                    
                src:RECONCILERUNKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_RECONCILERUN as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTFROM), '0'), 38, 10) is null and 
                    src:ACCOUNTFROM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTSRECONCILED), '0'), 38, 10) is null and 
                    src:ACCOUNTSRECONCILED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTTO), '0'), 38, 10) is null and 
                    src:ACCOUNTTO is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMMITNUMBER), '0'), 38, 10) is null and 
                    src:COMMITNUMBER is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:COMPLETEDDTTM), '1900-01-01')) is null and 
                    src:COMPLETEDDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:INITIATEDDTTM), '1900-01-01')) is null and 
                    src:INITIATEDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LASTINVOCATIONSTATUS), '0'), 38, 10) is null and 
                    src:LASTINVOCATIONSTATUS is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RECONCILERUNKEY), '0'), 38, 10) is null and 
                    src:RECONCILERUNKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SCHEDULEKEY), '0'), 38, 10) is null and 
                    src:SCHEDULEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TOTALNUMBEROFACCOUNTS), '0'), 38, 10) is null and 
                    src:TOTALNUMBEROFACCOUNTS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)