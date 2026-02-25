CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_BILLING_BILLRUN_ERROR AS SELECT src, 'IPS_BILLING_BILLRUN' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTKEY), '0'), 38, 10) is null and 
                    src:ACCOUNTKEY is not null) THEN
                    'ACCOUNTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ACCOUNTKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BILLDATE), '1900-01-01')) is null and 
                    src:BILLDATE is not null) THEN
                    'BILLDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:BILLDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BILLINGPERIODFROMDATE), '1900-01-01')) is null and 
                    src:BILLINGPERIODFROMDATE is not null) THEN
                    'BILLINGPERIODFROMDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:BILLINGPERIODFROMDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BILLINGPERIODTODATE), '1900-01-01')) is null and 
                    src:BILLINGPERIODTODATE is not null) THEN
                    'BILLINGPERIODTODATE contains a non-timestamp value : \'' || AS_VARCHAR(src:BILLINGPERIODTODATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLKEY), '0'), 38, 10) is null and 
                    src:BILLKEY is not null) THEN
                    'BILLKEY contains a non-numeric value : \'' || AS_VARCHAR(src:BILLKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLRUNKEY), '0'), 38, 10) is null and 
                    src:BILLRUNKEY is not null) THEN
                    'BILLRUNKEY contains a non-numeric value : \'' || AS_VARCHAR(src:BILLRUNKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLRUNSCHEDULEKEY), '0'), 38, 10) is null and 
                    src:BILLRUNSCHEDULEKEY is not null) THEN
                    'BILLRUNSCHEDULEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:BILLRUNSCHEDULEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLTYPEKEY), '0'), 38, 10) is null and 
                    src:BILLTYPEKEY is not null) THEN
                    'BILLTYPEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:BILLTYPEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CDRAPDEFNKEY), '0'), 38, 10) is null and 
                    src:CDRAPDEFNKEY is not null) THEN
                    'CDRAPDEFNKEY contains a non-numeric value : \'' || AS_VARCHAR(src:CDRAPDEFNKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CDRAPPROCESSSTATEKEY), '0'), 38, 10) is null and 
                    src:CDRAPPROCESSSTATEKEY is not null) THEN
                    'CDRAPPROCESSSTATEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:CDRAPPROCESSSTATEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CDRPRODFAMILY), '0'), 38, 10) is null and 
                    src:CDRPRODFAMILY is not null) THEN
                    'CDRPRODFAMILY contains a non-numeric value : \'' || AS_VARCHAR(src:CDRPRODFAMILY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMMITNUMBER), '0'), 38, 10) is null and 
                    src:COMMITNUMBER is not null) THEN
                    'COMMITNUMBER contains a non-numeric value : \'' || AS_VARCHAR(src:COMMITNUMBER) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DUEDATE), '1900-01-01')) is null and 
                    src:DUEDATE is not null) THEN
                    'DUEDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:DUEDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:INITIATEDTTM), '1900-01-01')) is null and 
                    src:INITIATEDTTM is not null) THEN
                    'INITIATEDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:INITIATEDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LASTINVOCATIONSTATUS), '0'), 38, 10) is null and 
                    src:LASTINVOCATIONSTATUS is not null) THEN
                    'LASTINVOCATIONSTATUS contains a non-numeric value : \'' || AS_VARCHAR(src:LASTINVOCATIONSTATUS) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMBEROFBILLSINRUN), '0'), 38, 10) is null and 
                    src:NUMBEROFBILLSINRUN is not null) THEN
                    'NUMBEROFBILLSINRUN contains a non-numeric value : \'' || AS_VARCHAR(src:NUMBEROFBILLSINRUN) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMBEROFEXCEPTIONS), '0'), 38, 10) is null and 
                    src:NUMBEROFEXCEPTIONS is not null) THEN
                    'NUMBEROFEXCEPTIONS contains a non-numeric value : \'' || AS_VARCHAR(src:NUMBEROFEXCEPTIONS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PRINCIPALTOTAL), '0'), 38, 10) is null and 
                    src:PRINCIPALTOTAL is not null) THEN
                    'PRINCIPALTOTAL contains a non-numeric value : \'' || AS_VARCHAR(src:PRINCIPALTOTAL) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REVERSEKEY), '0'), 38, 10) is null and 
                    src:REVERSEKEY is not null) THEN
                    'REVERSEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:REVERSEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ROUTEKEY), '0'), 38, 10) is null and 
                    src:ROUTEKEY is not null) THEN
                    'ROUTEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ROUTEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RUNAVERAGEBILLSPERMINUTE), '0'), 38, 10) is null and 
                    src:RUNAVERAGEBILLSPERMINUTE is not null) THEN
                    'RUNAVERAGEBILLSPERMINUTE contains a non-numeric value : \'' || AS_VARCHAR(src:RUNAVERAGEBILLSPERMINUTE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SCHEDULEKEY), '0'), 38, 10) is null and 
                    src:SCHEDULEKEY is not null) THEN
                    'SCHEDULEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:SCHEDULEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TOTALNUMBEROFBILLS), '0'), 38, 10) is null and 
                    src:TOTALNUMBEROFBILLS is not null) THEN
                    'TOTALNUMBEROFBILLS contains a non-numeric value : \'' || AS_VARCHAR(src:TOTALNUMBEROFBILLS) || '\'' WHEN 
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
                                    
                src:BILLRUNKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_BILLRUN as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTKEY), '0'), 38, 10) is null and 
                    src:ACCOUNTKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BILLDATE), '1900-01-01')) is null and 
                    src:BILLDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BILLINGPERIODFROMDATE), '1900-01-01')) is null and 
                    src:BILLINGPERIODFROMDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BILLINGPERIODTODATE), '1900-01-01')) is null and 
                    src:BILLINGPERIODTODATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLKEY), '0'), 38, 10) is null and 
                    src:BILLKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLRUNKEY), '0'), 38, 10) is null and 
                    src:BILLRUNKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLRUNSCHEDULEKEY), '0'), 38, 10) is null and 
                    src:BILLRUNSCHEDULEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLTYPEKEY), '0'), 38, 10) is null and 
                    src:BILLTYPEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CDRAPDEFNKEY), '0'), 38, 10) is null and 
                    src:CDRAPDEFNKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CDRAPPROCESSSTATEKEY), '0'), 38, 10) is null and 
                    src:CDRAPPROCESSSTATEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CDRPRODFAMILY), '0'), 38, 10) is null and 
                    src:CDRPRODFAMILY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMMITNUMBER), '0'), 38, 10) is null and 
                    src:COMMITNUMBER is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DUEDATE), '1900-01-01')) is null and 
                    src:DUEDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:INITIATEDTTM), '1900-01-01')) is null and 
                    src:INITIATEDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LASTINVOCATIONSTATUS), '0'), 38, 10) is null and 
                    src:LASTINVOCATIONSTATUS is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMBEROFBILLSINRUN), '0'), 38, 10) is null and 
                    src:NUMBEROFBILLSINRUN is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMBEROFEXCEPTIONS), '0'), 38, 10) is null and 
                    src:NUMBEROFEXCEPTIONS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PRINCIPALTOTAL), '0'), 38, 10) is null and 
                    src:PRINCIPALTOTAL is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REVERSEKEY), '0'), 38, 10) is null and 
                    src:REVERSEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ROUTEKEY), '0'), 38, 10) is null and 
                    src:ROUTEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RUNAVERAGEBILLSPERMINUTE), '0'), 38, 10) is null and 
                    src:RUNAVERAGEBILLSPERMINUTE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SCHEDULEKEY), '0'), 38, 10) is null and 
                    src:SCHEDULEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TOTALNUMBEROFBILLS), '0'), 38, 10) is null and 
                    src:TOTALNUMBEROFBILLS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)