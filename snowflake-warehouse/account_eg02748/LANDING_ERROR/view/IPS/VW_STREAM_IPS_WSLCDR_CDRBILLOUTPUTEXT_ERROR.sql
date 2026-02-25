CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_WSLCDR_CDRBILLOUTPUTEXT_ERROR AS SELECT src, 'IPS_WSLCDR_CDRBILLOUTPUTEXT' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTKEY), '0'), 38, 10) is null and 
                    src:ACCOUNTKEY is not null) THEN
                    'ACCOUNTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ACCOUNTKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADJUSTMENTAMOUNT), '0'), 38, 10) is null and 
                    src:ADJUSTMENTAMOUNT is not null) THEN
                    'ADJUSTMENTAMOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:ADJUSTMENTAMOUNT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BALANCECURRENTCHARGE), '0'), 38, 10) is null and 
                    src:BALANCECURRENTCHARGE is not null) THEN
                    'BALANCECURRENTCHARGE contains a non-numeric value : \'' || AS_VARCHAR(src:BALANCECURRENTCHARGE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BARCODEDUEDATE), '1900-01-01')) is null and 
                    src:BARCODEDUEDATE is not null) THEN
                    'BARCODEDUEDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:BARCODEDUEDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLAMOUNT), '0'), 38, 10) is null and 
                    src:BILLAMOUNT is not null) THEN
                    'BILLAMOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:BILLAMOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BILLDATE), '1900-01-01')) is null and 
                    src:BILLDATE is not null) THEN
                    'BILLDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:BILLDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLRUNKEY), '0'), 38, 10) is null and 
                    src:BILLRUNKEY is not null) THEN
                    'BILLRUNKEY contains a non-numeric value : \'' || AS_VARCHAR(src:BILLRUNKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BRFWRD), '0'), 38, 10) is null and 
                    src:BRFWRD is not null) THEN
                    'BRFWRD contains a non-numeric value : \'' || AS_VARCHAR(src:BRFWRD) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CDRBILLOUTPUTEXTKEY), '0'), 38, 10) is null and 
                    src:CDRBILLOUTPUTEXTKEY is not null) THEN
                    'CDRBILLOUTPUTEXTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:CDRBILLOUTPUTEXTKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CONTROLTOTAL), '0'), 38, 10) is null and 
                    src:CONTROLTOTAL is not null) THEN
                    'CONTROLTOTAL contains a non-numeric value : \'' || AS_VARCHAR(src:CONTROLTOTAL) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CURRENTBILLKEY), '0'), 38, 10) is null and 
                    src:CURRENTBILLKEY is not null) THEN
                    'CURRENTBILLKEY contains a non-numeric value : \'' || AS_VARCHAR(src:CURRENTBILLKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CURRPENAMT), '0'), 38, 10) is null and 
                    src:CURRPENAMT is not null) THEN
                    'CURRPENAMT contains a non-numeric value : \'' || AS_VARCHAR(src:CURRPENAMT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DISCAMOUNTDUE), '0'), 38, 10) is null and 
                    src:DISCAMOUNTDUE is not null) THEN
                    'DISCAMOUNTDUE contains a non-numeric value : \'' || AS_VARCHAR(src:DISCAMOUNTDUE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DISCTOTALDUE), '0'), 38, 10) is null and 
                    src:DISCTOTALDUE is not null) THEN
                    'DISCTOTALDUE contains a non-numeric value : \'' || AS_VARCHAR(src:DISCTOTALDUE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DUEDATE), '1900-01-01')) is null and 
                    src:DUEDATE is not null) THEN
                    'DUEDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:DUEDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:GSTAMOUNT), '0'), 38, 10) is null and 
                    src:GSTAMOUNT is not null) THEN
                    'GSTAMOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:GSTAMOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:LASTPAYMENTDATE), '1900-01-01')) is null and 
                    src:LASTPAYMENTDATE is not null) THEN
                    'LASTPAYMENTDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:LASTPAYMENTDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMBEROFINVOICES), '0'), 38, 10) is null and 
                    src:NUMBEROFINVOICES is not null) THEN
                    'NUMBEROFINVOICES contains a non-numeric value : \'' || AS_VARCHAR(src:NUMBEROFINVOICES) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OPENBALANCE), '0'), 38, 10) is null and 
                    src:OPENBALANCE is not null) THEN
                    'OPENBALANCE contains a non-numeric value : \'' || AS_VARCHAR(src:OPENBALANCE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PARENTACCTKEY), '0'), 38, 10) is null and 
                    src:PARENTACCTKEY is not null) THEN
                    'PARENTACCTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:PARENTACCTKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYMENTAMOUNT), '0'), 38, 10) is null and 
                    src:PAYMENTAMOUNT is not null) THEN
                    'PAYMENTAMOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:PAYMENTAMOUNT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PREVBILLAMOUNT), '0'), 38, 10) is null and 
                    src:PREVBILLAMOUNT is not null) THEN
                    'PREVBILLAMOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:PREVBILLAMOUNT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PRINCIPALTOTAL), '0'), 38, 10) is null and 
                    src:PRINCIPALTOTAL is not null) THEN
                    'PRINCIPALTOTAL contains a non-numeric value : \'' || AS_VARCHAR(src:PRINCIPALTOTAL) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RUNNUMBER), '0'), 38, 10) is null and 
                    src:RUNNUMBER is not null) THEN
                    'RUNNUMBER contains a non-numeric value : \'' || AS_VARCHAR(src:RUNNUMBER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TOTALDUE), '0'), 38, 10) is null and 
                    src:TOTALDUE is not null) THEN
                    'TOTALDUE contains a non-numeric value : \'' || AS_VARCHAR(src:TOTALDUE) || '\'' WHEN 
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
                                    
                src:CDRBILLOUTPUTEXTKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_WSLCDR_CDRBILLOUTPUTEXT as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTKEY), '0'), 38, 10) is null and 
                    src:ACCOUNTKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADJUSTMENTAMOUNT), '0'), 38, 10) is null and 
                    src:ADJUSTMENTAMOUNT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BALANCECURRENTCHARGE), '0'), 38, 10) is null and 
                    src:BALANCECURRENTCHARGE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BARCODEDUEDATE), '1900-01-01')) is null and 
                    src:BARCODEDUEDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLAMOUNT), '0'), 38, 10) is null and 
                    src:BILLAMOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BILLDATE), '1900-01-01')) is null and 
                    src:BILLDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLRUNKEY), '0'), 38, 10) is null and 
                    src:BILLRUNKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BRFWRD), '0'), 38, 10) is null and 
                    src:BRFWRD is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CDRBILLOUTPUTEXTKEY), '0'), 38, 10) is null and 
                    src:CDRBILLOUTPUTEXTKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CONTROLTOTAL), '0'), 38, 10) is null and 
                    src:CONTROLTOTAL is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CURRENTBILLKEY), '0'), 38, 10) is null and 
                    src:CURRENTBILLKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CURRPENAMT), '0'), 38, 10) is null and 
                    src:CURRPENAMT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DISCAMOUNTDUE), '0'), 38, 10) is null and 
                    src:DISCAMOUNTDUE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DISCTOTALDUE), '0'), 38, 10) is null and 
                    src:DISCTOTALDUE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DUEDATE), '1900-01-01')) is null and 
                    src:DUEDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:GSTAMOUNT), '0'), 38, 10) is null and 
                    src:GSTAMOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:LASTPAYMENTDATE), '1900-01-01')) is null and 
                    src:LASTPAYMENTDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMBEROFINVOICES), '0'), 38, 10) is null and 
                    src:NUMBEROFINVOICES is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OPENBALANCE), '0'), 38, 10) is null and 
                    src:OPENBALANCE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PARENTACCTKEY), '0'), 38, 10) is null and 
                    src:PARENTACCTKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYMENTAMOUNT), '0'), 38, 10) is null and 
                    src:PAYMENTAMOUNT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PREVBILLAMOUNT), '0'), 38, 10) is null and 
                    src:PREVBILLAMOUNT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PRINCIPALTOTAL), '0'), 38, 10) is null and 
                    src:PRINCIPALTOTAL is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RUNNUMBER), '0'), 38, 10) is null and 
                    src:RUNNUMBER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TOTALDUE), '0'), 38, 10) is null and 
                    src:TOTALDUE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)