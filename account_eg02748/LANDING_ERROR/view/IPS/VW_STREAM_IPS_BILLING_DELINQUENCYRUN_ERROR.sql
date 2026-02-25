CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_BILLING_DELINQUENCYRUN_ERROR AS SELECT src, 'IPS_BILLING_DELINQUENCYRUN' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLTYPEKEY), '0'), 38, 10) is null and 
                    src:BILLTYPEKEY is not null) THEN
                    'BILLTYPEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:BILLTYPEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMMITNUMBER), '0'), 38, 10) is null and 
                    src:COMMITNUMBER is not null) THEN
                    'COMMITNUMBER contains a non-numeric value : \'' || AS_VARCHAR(src:COMMITNUMBER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DELINQUENCYRUNKEY), '0'), 38, 10) is null and 
                    src:DELINQUENCYRUNKEY is not null) THEN
                    'DELINQUENCYRUNKEY contains a non-numeric value : \'' || AS_VARCHAR(src:DELINQUENCYRUNKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ENDADVANCEMENTRUN), '1900-01-01')) is null and 
                    src:ENDADVANCEMENTRUN is not null) THEN
                    'ENDADVANCEMENTRUN contains a non-timestamp value : \'' || AS_VARCHAR(src:ENDADVANCEMENTRUN) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ENDENTRYRUN), '1900-01-01')) is null and 
                    src:ENDENTRYRUN is not null) THEN
                    'ENDENTRYRUN contains a non-timestamp value : \'' || AS_VARCHAR(src:ENDENTRYRUN) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:INITIATEDTTM), '1900-01-01')) is null and 
                    src:INITIATEDTTM is not null) THEN
                    'INITIATEDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:INITIATEDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LASTINVOCATIONSTATUS), '0'), 38, 10) is null and 
                    src:LASTINVOCATIONSTATUS is not null) THEN
                    'LASTINVOCATIONSTATUS contains a non-numeric value : \'' || AS_VARCHAR(src:LASTINVOCATIONSTATUS) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMBEROFADVANCEDACCOUNTS), '0'), 38, 10) is null and 
                    src:NUMBEROFADVANCEDACCOUNTS is not null) THEN
                    'NUMBEROFADVANCEDACCOUNTS contains a non-numeric value : \'' || AS_VARCHAR(src:NUMBEROFADVANCEDACCOUNTS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMBEROFENTRYACCOUNTS), '0'), 38, 10) is null and 
                    src:NUMBEROFENTRYACCOUNTS is not null) THEN
                    'NUMBEROFENTRYACCOUNTS contains a non-numeric value : \'' || AS_VARCHAR(src:NUMBEROFENTRYACCOUNTS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMBEROFEXCEPTIONS), '0'), 38, 10) is null and 
                    src:NUMBEROFEXCEPTIONS is not null) THEN
                    'NUMBEROFEXCEPTIONS contains a non-numeric value : \'' || AS_VARCHAR(src:NUMBEROFEXCEPTIONS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SCHEDULEKEY), '0'), 38, 10) is null and 
                    src:SCHEDULEKEY is not null) THEN
                    'SCHEDULEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:SCHEDULEKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STARTADVANCEMENTRUN), '1900-01-01')) is null and 
                    src:STARTADVANCEMENTRUN is not null) THEN
                    'STARTADVANCEMENTRUN contains a non-timestamp value : \'' || AS_VARCHAR(src:STARTADVANCEMENTRUN) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STARTENTRYRUN), '1900-01-01')) is null and 
                    src:STARTENTRYRUN is not null) THEN
                    'STARTENTRYRUN contains a non-timestamp value : \'' || AS_VARCHAR(src:STARTENTRYRUN) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TOTALNUMBEROFACCOUNTS), '0'), 38, 10) is null and 
                    src:TOTALNUMBEROFACCOUNTS is not null) THEN
                    'TOTALNUMBEROFACCOUNTS contains a non-numeric value : \'' || AS_VARCHAR(src:TOTALNUMBEROFACCOUNTS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) THEN
                    'VARIATION_ID contains a non-numeric value : \'' || AS_VARCHAR(src:VARIATION_ID) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WATERMETERADDRESSROUTEKEY), '0'), 38, 10) is null and 
                    src:WATERMETERADDRESSROUTEKEY is not null) THEN
                    'WATERMETERADDRESSROUTEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:WATERMETERADDRESSROUTEKEY) || '\'' WHEN 
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
                                    
                src:DELINQUENCYRUNKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_DELINQUENCYRUN as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLTYPEKEY), '0'), 38, 10) is null and 
                    src:BILLTYPEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMMITNUMBER), '0'), 38, 10) is null and 
                    src:COMMITNUMBER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DELINQUENCYRUNKEY), '0'), 38, 10) is null and 
                    src:DELINQUENCYRUNKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ENDADVANCEMENTRUN), '1900-01-01')) is null and 
                    src:ENDADVANCEMENTRUN is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ENDENTRYRUN), '1900-01-01')) is null and 
                    src:ENDENTRYRUN is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:INITIATEDTTM), '1900-01-01')) is null and 
                    src:INITIATEDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LASTINVOCATIONSTATUS), '0'), 38, 10) is null and 
                    src:LASTINVOCATIONSTATUS is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMBEROFADVANCEDACCOUNTS), '0'), 38, 10) is null and 
                    src:NUMBEROFADVANCEDACCOUNTS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMBEROFENTRYACCOUNTS), '0'), 38, 10) is null and 
                    src:NUMBEROFENTRYACCOUNTS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMBEROFEXCEPTIONS), '0'), 38, 10) is null and 
                    src:NUMBEROFEXCEPTIONS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SCHEDULEKEY), '0'), 38, 10) is null and 
                    src:SCHEDULEKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STARTADVANCEMENTRUN), '1900-01-01')) is null and 
                    src:STARTADVANCEMENTRUN is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STARTENTRYRUN), '1900-01-01')) is null and 
                    src:STARTENTRYRUN is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TOTALNUMBEROFACCOUNTS), '0'), 38, 10) is null and 
                    src:TOTALNUMBEROFACCOUNTS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WATERMETERADDRESSROUTEKEY), '0'), 38, 10) is null and 
                    src:WATERMETERADDRESSROUTEKEY is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)