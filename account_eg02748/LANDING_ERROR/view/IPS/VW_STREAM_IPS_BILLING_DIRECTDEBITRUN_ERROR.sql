CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_BILLING_DIRECTDEBITRUN_ERROR AS SELECT src, 'IPS_BILLING_DIRECTDEBITRUN' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACHENTRYAMOUNT), '0'), 38, 10) is null and 
                    src:ACHENTRYAMOUNT is not null) THEN
                    'ACHENTRYAMOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:ACHENTRYAMOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLRUNKEY), '0'), 38, 10) is null and 
                    src:BILLRUNKEY is not null) THEN
                    'BILLRUNKEY contains a non-numeric value : \'' || AS_VARCHAR(src:BILLRUNKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLTYPEKEY), '0'), 38, 10) is null and 
                    src:BILLTYPEKEY is not null) THEN
                    'BILLTYPEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:BILLTYPEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMMENTSKEY), '0'), 38, 10) is null and 
                    src:COMMENTSKEY is not null) THEN
                    'COMMENTSKEY contains a non-numeric value : \'' || AS_VARCHAR(src:COMMENTSKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DIRECTDEBITRUNKEY), '0'), 38, 10) is null and 
                    src:DIRECTDEBITRUNKEY is not null) THEN
                    'DIRECTDEBITRUNKEY contains a non-numeric value : \'' || AS_VARCHAR(src:DIRECTDEBITRUNKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EXTRACTTHROUGHDATE), '1900-01-01')) is null and 
                    src:EXTRACTTHROUGHDATE is not null) THEN
                    'EXTRACTTHROUGHDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:EXTRACTTHROUGHDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LASTINVOCATIONSTATUS), '0'), 38, 10) is null and 
                    src:LASTINVOCATIONSTATUS is not null) THEN
                    'LASTINVOCATIONSTATUS contains a non-numeric value : \'' || AS_VARCHAR(src:LASTINVOCATIONSTATUS) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMBEROFACCOUNTS), '0'), 38, 10) is null and 
                    src:NUMBEROFACCOUNTS is not null) THEN
                    'NUMBEROFACCOUNTS contains a non-numeric value : \'' || AS_VARCHAR(src:NUMBEROFACCOUNTS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMBEROFACHENTRIES), '0'), 38, 10) is null and 
                    src:NUMBEROFACHENTRIES is not null) THEN
                    'NUMBEROFACHENTRIES contains a non-numeric value : \'' || AS_VARCHAR(src:NUMBEROFACHENTRIES) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMBEROFBILLS), '0'), 38, 10) is null and 
                    src:NUMBEROFBILLS is not null) THEN
                    'NUMBEROFBILLS contains a non-numeric value : \'' || AS_VARCHAR(src:NUMBEROFBILLS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMBEROFDEBITSCHEDULE), '0'), 38, 10) is null and 
                    src:NUMBEROFDEBITSCHEDULE is not null) THEN
                    'NUMBEROFDEBITSCHEDULE contains a non-numeric value : \'' || AS_VARCHAR(src:NUMBEROFDEBITSCHEDULE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYMENTBATCHCOUNT), '0'), 38, 10) is null and 
                    src:PAYMENTBATCHCOUNT is not null) THEN
                    'PAYMENTBATCHCOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:PAYMENTBATCHCOUNT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PRENOTEBANKACCOUNTCOUNT), '0'), 38, 10) is null and 
                    src:PRENOTEBANKACCOUNTCOUNT is not null) THEN
                    'PRENOTEBANKACCOUNTCOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:PRENOTEBANKACCOUNTCOUNT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SCHEDULEKEY), '0'), 38, 10) is null and 
                    src:SCHEDULEKEY is not null) THEN
                    'SCHEDULEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:SCHEDULEKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STARTDTTM), '1900-01-01')) is null and 
                    src:STARTDTTM is not null) THEN
                    'STARTDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:STARTDTTM) || '\'' WHEN 
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
                                    
                src:DIRECTDEBITRUNKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_DIRECTDEBITRUN as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACHENTRYAMOUNT), '0'), 38, 10) is null and 
                    src:ACHENTRYAMOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLRUNKEY), '0'), 38, 10) is null and 
                    src:BILLRUNKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLTYPEKEY), '0'), 38, 10) is null and 
                    src:BILLTYPEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMMENTSKEY), '0'), 38, 10) is null and 
                    src:COMMENTSKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DIRECTDEBITRUNKEY), '0'), 38, 10) is null and 
                    src:DIRECTDEBITRUNKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EXTRACTTHROUGHDATE), '1900-01-01')) is null and 
                    src:EXTRACTTHROUGHDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LASTINVOCATIONSTATUS), '0'), 38, 10) is null and 
                    src:LASTINVOCATIONSTATUS is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMBEROFACCOUNTS), '0'), 38, 10) is null and 
                    src:NUMBEROFACCOUNTS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMBEROFACHENTRIES), '0'), 38, 10) is null and 
                    src:NUMBEROFACHENTRIES is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMBEROFBILLS), '0'), 38, 10) is null and 
                    src:NUMBEROFBILLS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMBEROFDEBITSCHEDULE), '0'), 38, 10) is null and 
                    src:NUMBEROFDEBITSCHEDULE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYMENTBATCHCOUNT), '0'), 38, 10) is null and 
                    src:PAYMENTBATCHCOUNT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PRENOTEBANKACCOUNTCOUNT), '0'), 38, 10) is null and 
                    src:PRENOTEBANKACCOUNTCOUNT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SCHEDULEKEY), '0'), 38, 10) is null and 
                    src:SCHEDULEKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STARTDTTM), '1900-01-01')) is null and 
                    src:STARTDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)