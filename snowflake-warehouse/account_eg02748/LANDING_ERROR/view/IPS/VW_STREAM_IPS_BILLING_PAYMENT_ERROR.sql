CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_BILLING_PAYMENT_ERROR AS SELECT src, 'IPS_BILLING_PAYMENT' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTKEY), '0'), 38, 10) is null and 
                    src:ACCOUNTKEY is not null) THEN
                    'ACCOUNTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ACCOUNTKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTTYPE), '0'), 38, 10) is null and 
                    src:ACCOUNTTYPE is not null) THEN
                    'ACCOUNTTYPE contains a non-numeric value : \'' || AS_VARCHAR(src:ACCOUNTTYPE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AMOUNT), '0'), 38, 10) is null and 
                    src:AMOUNT is not null) THEN
                    'AMOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:AMOUNT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AMTCONF), '0'), 38, 10) is null and 
                    src:AMTCONF is not null) THEN
                    'AMTCONF contains a non-numeric value : \'' || AS_VARCHAR(src:AMTCONF) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BATCHKEY), '0'), 38, 10) is null and 
                    src:BATCHKEY is not null) THEN
                    'BATCHKEY contains a non-numeric value : \'' || AS_VARCHAR(src:BATCHKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLTYPE), '0'), 38, 10) is null and 
                    src:BILLTYPE is not null) THEN
                    'BILLTYPE contains a non-numeric value : \'' || AS_VARCHAR(src:BILLTYPE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CARDEXPIREDATE), '1900-01-01')) is null and 
                    src:CARDEXPIREDATE is not null) THEN
                    'CARDEXPIREDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:CARDEXPIREDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CNTCTKEY), '0'), 38, 10) is null and 
                    src:CNTCTKEY is not null) THEN
                    'CNTCTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:CNTCTKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CONFDTTM), '1900-01-01')) is null and 
                    src:CONFDTTM is not null) THEN
                    'CONFDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:CONFDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DONATEAMT), '0'), 38, 10) is null and 
                    src:DONATEAMT is not null) THEN
                    'DONATEAMT contains a non-numeric value : \'' || AS_VARCHAR(src:DONATEAMT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DRAWERTRANNO), '0'), 38, 10) is null and 
                    src:DRAWERTRANNO is not null) THEN
                    'DRAWERTRANNO contains a non-numeric value : \'' || AS_VARCHAR(src:DRAWERTRANNO) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ESCROWACCOUNTKEY), '0'), 38, 10) is null and 
                    src:ESCROWACCOUNTKEY is not null) THEN
                    'ESCROWACCOUNTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ESCROWACCOUNTKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ESCROWTRNNO), '0'), 38, 10) is null and 
                    src:ESCROWTRNNO is not null) THEN
                    'ESCROWTRNNO contains a non-numeric value : \'' || AS_VARCHAR(src:ESCROWTRNNO) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYKEY), '0'), 38, 10) is null and 
                    src:PAYKEY is not null) THEN
                    'PAYKEY contains a non-numeric value : \'' || AS_VARCHAR(src:PAYKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:RECDDTTM), '1900-01-01')) is null and 
                    src:RECDDTTM is not null) THEN
                    'RECDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:RECDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REGTRANNO), '0'), 38, 10) is null and 
                    src:REGTRANNO is not null) THEN
                    'REGTRANNO contains a non-numeric value : \'' || AS_VARCHAR(src:REGTRANNO) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:RESDTTM), '1900-01-01')) is null and 
                    src:RESDTTM is not null) THEN
                    'RESDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:RESDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRACENO), '0'), 38, 10) is null and 
                    src:TRACENO is not null) THEN
                    'TRACENO contains a non-numeric value : \'' || AS_VARCHAR(src:TRACENO) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRANSFERREDFROMPAYMENT), '0'), 38, 10) is null and 
                    src:TRANSFERREDFROMPAYMENT is not null) THEN
                    'TRANSFERREDFROMPAYMENT contains a non-numeric value : \'' || AS_VARCHAR(src:TRANSFERREDFROMPAYMENT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:UNMATCHEDPROCESSDATETIME), '1900-01-01')) is null and 
                    src:UNMATCHEDPROCESSDATETIME is not null) THEN
                    'UNMATCHEDPROCESSDATETIME contains a non-timestamp value : \'' || AS_VARCHAR(src:UNMATCHEDPROCESSDATETIME) || '\'' WHEN 
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
                                    
                src:PAYKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_PAYMENT as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTKEY), '0'), 38, 10) is null and 
                    src:ACCOUNTKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTTYPE), '0'), 38, 10) is null and 
                    src:ACCOUNTTYPE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AMOUNT), '0'), 38, 10) is null and 
                    src:AMOUNT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AMTCONF), '0'), 38, 10) is null and 
                    src:AMTCONF is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BATCHKEY), '0'), 38, 10) is null and 
                    src:BATCHKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLTYPE), '0'), 38, 10) is null and 
                    src:BILLTYPE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CARDEXPIREDATE), '1900-01-01')) is null and 
                    src:CARDEXPIREDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CNTCTKEY), '0'), 38, 10) is null and 
                    src:CNTCTKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CONFDTTM), '1900-01-01')) is null and 
                    src:CONFDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DONATEAMT), '0'), 38, 10) is null and 
                    src:DONATEAMT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DRAWERTRANNO), '0'), 38, 10) is null and 
                    src:DRAWERTRANNO is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ESCROWACCOUNTKEY), '0'), 38, 10) is null and 
                    src:ESCROWACCOUNTKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ESCROWTRNNO), '0'), 38, 10) is null and 
                    src:ESCROWTRNNO is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYKEY), '0'), 38, 10) is null and 
                    src:PAYKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:RECDDTTM), '1900-01-01')) is null and 
                    src:RECDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REGTRANNO), '0'), 38, 10) is null and 
                    src:REGTRANNO is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:RESDTTM), '1900-01-01')) is null and 
                    src:RESDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRACENO), '0'), 38, 10) is null and 
                    src:TRACENO is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRANSFERREDFROMPAYMENT), '0'), 38, 10) is null and 
                    src:TRANSFERREDFROMPAYMENT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:UNMATCHEDPROCESSDATETIME), '1900-01-01')) is null and 
                    src:UNMATCHEDPROCESSDATETIME is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)