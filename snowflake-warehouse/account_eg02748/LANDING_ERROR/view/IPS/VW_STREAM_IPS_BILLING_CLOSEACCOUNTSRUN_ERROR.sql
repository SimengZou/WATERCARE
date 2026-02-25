CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_BILLING_CLOSEACCOUNTSRUN_ERROR AS SELECT src, 'IPS_BILLING_CLOSEACCOUNTSRUN' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALERTKEY), '0'), 38, 10) is null and 
                    src:ALERTKEY is not null) THEN
                    'ALERTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ALERTKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CLOSEACCOUNTSRUNKEY), '0'), 38, 10) is null and 
                    src:CLOSEACCOUNTSRUNKEY is not null) THEN
                    'CLOSEACCOUNTSRUNKEY contains a non-numeric value : \'' || AS_VARCHAR(src:CLOSEACCOUNTSRUNKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CONDITIONFORMULAKEY), '0'), 38, 10) is null and 
                    src:CONDITIONFORMULAKEY is not null) THEN
                    'CONDITIONFORMULAKEY contains a non-numeric value : \'' || AS_VARCHAR(src:CONDITIONFORMULAKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DAYSWITHOUTTRANSACTIONS), '0'), 38, 10) is null and 
                    src:DAYSWITHOUTTRANSACTIONS is not null) THEN
                    'DAYSWITHOUTTRANSACTIONS contains a non-numeric value : \'' || AS_VARCHAR(src:DAYSWITHOUTTRANSACTIONS) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ENDRUN), '1900-01-01')) is null and 
                    src:ENDRUN is not null) THEN
                    'ENDRUN contains a non-timestamp value : \'' || AS_VARCHAR(src:ENDRUN) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LASTINVOCATIONSTATUS), '0'), 38, 10) is null and 
                    src:LASTINVOCATIONSTATUS is not null) THEN
                    'LASTINVOCATIONSTATUS contains a non-numeric value : \'' || AS_VARCHAR(src:LASTINVOCATIONSTATUS) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMBEROFACCOUNTSCLOSED), '0'), 38, 10) is null and 
                    src:NUMBEROFACCOUNTSCLOSED is not null) THEN
                    'NUMBEROFACCOUNTSCLOSED contains a non-numeric value : \'' || AS_VARCHAR(src:NUMBEROFACCOUNTSCLOSED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMBEROFEXCEPTIONS), '0'), 38, 10) is null and 
                    src:NUMBEROFEXCEPTIONS is not null) THEN
                    'NUMBEROFEXCEPTIONS contains a non-numeric value : \'' || AS_VARCHAR(src:NUMBEROFEXCEPTIONS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SCHEDULEKEY), '0'), 38, 10) is null and 
                    src:SCHEDULEKEY is not null) THEN
                    'SCHEDULEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:SCHEDULEKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STARTRUN), '1900-01-01')) is null and 
                    src:STARTRUN is not null) THEN
                    'STARTRUN contains a non-timestamp value : \'' || AS_VARCHAR(src:STARTRUN) || '\'' WHEN 
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
                                    
                src:CLOSEACCOUNTSRUNKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_CLOSEACCOUNTSRUN as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ALERTKEY), '0'), 38, 10) is null and 
                    src:ALERTKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CLOSEACCOUNTSRUNKEY), '0'), 38, 10) is null and 
                    src:CLOSEACCOUNTSRUNKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CONDITIONFORMULAKEY), '0'), 38, 10) is null and 
                    src:CONDITIONFORMULAKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DAYSWITHOUTTRANSACTIONS), '0'), 38, 10) is null and 
                    src:DAYSWITHOUTTRANSACTIONS is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ENDRUN), '1900-01-01')) is null and 
                    src:ENDRUN is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LASTINVOCATIONSTATUS), '0'), 38, 10) is null and 
                    src:LASTINVOCATIONSTATUS is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMBEROFACCOUNTSCLOSED), '0'), 38, 10) is null and 
                    src:NUMBEROFACCOUNTSCLOSED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMBEROFEXCEPTIONS), '0'), 38, 10) is null and 
                    src:NUMBEROFEXCEPTIONS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SCHEDULEKEY), '0'), 38, 10) is null and 
                    src:SCHEDULEKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STARTRUN), '1900-01-01')) is null and 
                    src:STARTRUN is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)