CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_BILLING_ACCTTRAN_ERROR AS SELECT src, 'IPS_BILLING_ACCTTRAN' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTKEY), '0'), 38, 10) is null and 
                    src:ACCOUNTKEY is not null) THEN
                    'ACCOUNTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ACCOUNTKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADJUSTMENTKEY), '0'), 38, 10) is null and 
                    src:ADJUSTMENTKEY is not null) THEN
                    'ADJUSTMENTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ADJUSTMENTKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPFEEKEY), '0'), 38, 10) is null and 
                    src:APPFEEKEY is not null) THEN
                    'APPFEEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:APPFEEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMMENTSKEY), '0'), 38, 10) is null and 
                    src:COMMENTSKEY is not null) THEN
                    'COMMENTSKEY contains a non-numeric value : \'' || AS_VARCHAR(src:COMMENTSKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DEPOSITTRANSACTIONKEY), '0'), 38, 10) is null and 
                    src:DEPOSITTRANSACTIONKEY is not null) THEN
                    'DEPOSITTRANSACTIONKEY contains a non-numeric value : \'' || AS_VARCHAR(src:DEPOSITTRANSACTIONKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LINEITEMKEY), '0'), 38, 10) is null and 
                    src:LINEITEMKEY is not null) THEN
                    'LINEITEMKEY contains a non-numeric value : \'' || AS_VARCHAR(src:LINEITEMKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ONEOFFCHARGEKEY), '0'), 38, 10) is null and 
                    src:ONEOFFCHARGEKEY is not null) THEN
                    'ONEOFFCHARGEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ONEOFFCHARGEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYKEY), '0'), 38, 10) is null and 
                    src:PAYKEY is not null) THEN
                    'PAYKEY contains a non-numeric value : \'' || AS_VARCHAR(src:PAYKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PENALTYKEY), '0'), 38, 10) is null and 
                    src:PENALTYKEY is not null) THEN
                    'PENALTYKEY contains a non-numeric value : \'' || AS_VARCHAR(src:PENALTYKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REFTRANNO), '0'), 38, 10) is null and 
                    src:REFTRANNO is not null) THEN
                    'REFTRANNO contains a non-numeric value : \'' || AS_VARCHAR(src:REFTRANNO) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REFUNDKEY), '0'), 38, 10) is null and 
                    src:REFUNDKEY is not null) THEN
                    'REFUNDKEY contains a non-numeric value : \'' || AS_VARCHAR(src:REFUNDKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SOURCEKEY), '0'), 38, 10) is null and 
                    src:SOURCEKEY is not null) THEN
                    'SOURCEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:SOURCEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SOURCETYPE), '0'), 38, 10) is null and 
                    src:SOURCETYPE is not null) THEN
                    'SOURCETYPE contains a non-numeric value : \'' || AS_VARCHAR(src:SOURCETYPE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRANAMT), '0'), 38, 10) is null and 
                    src:TRANAMT is not null) THEN
                    'TRANAMT contains a non-numeric value : \'' || AS_VARCHAR(src:TRANAMT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRANDATE), '1900-01-01')) is null and 
                    src:TRANDATE is not null) THEN
                    'TRANDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:TRANDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRANNO), '0'), 38, 10) is null and 
                    src:TRANNO is not null) THEN
                    'TRANNO contains a non-numeric value : \'' || AS_VARCHAR(src:TRANNO) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRANSACTIONDESIGNATOR), '0'), 38, 10) is null and 
                    src:TRANSACTIONDESIGNATOR is not null) THEN
                    'TRANSACTIONDESIGNATOR contains a non-numeric value : \'' || AS_VARCHAR(src:TRANSACTIONDESIGNATOR) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRANSACTIONTYPE), '0'), 38, 10) is null and 
                    src:TRANSACTIONTYPE is not null) THEN
                    'TRANSACTIONTYPE contains a non-numeric value : \'' || AS_VARCHAR(src:TRANSACTIONTYPE) || '\'' WHEN 
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
                                    
                src:TRANNO  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_ACCTTRAN as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTKEY), '0'), 38, 10) is null and 
                    src:ACCOUNTKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADJUSTMENTKEY), '0'), 38, 10) is null and 
                    src:ADJUSTMENTKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPFEEKEY), '0'), 38, 10) is null and 
                    src:APPFEEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMMENTSKEY), '0'), 38, 10) is null and 
                    src:COMMENTSKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DEPOSITTRANSACTIONKEY), '0'), 38, 10) is null and 
                    src:DEPOSITTRANSACTIONKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LINEITEMKEY), '0'), 38, 10) is null and 
                    src:LINEITEMKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ONEOFFCHARGEKEY), '0'), 38, 10) is null and 
                    src:ONEOFFCHARGEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYKEY), '0'), 38, 10) is null and 
                    src:PAYKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PENALTYKEY), '0'), 38, 10) is null and 
                    src:PENALTYKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REFTRANNO), '0'), 38, 10) is null and 
                    src:REFTRANNO is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REFUNDKEY), '0'), 38, 10) is null and 
                    src:REFUNDKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SOURCEKEY), '0'), 38, 10) is null and 
                    src:SOURCEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SOURCETYPE), '0'), 38, 10) is null and 
                    src:SOURCETYPE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRANAMT), '0'), 38, 10) is null and 
                    src:TRANAMT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRANDATE), '1900-01-01')) is null and 
                    src:TRANDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRANNO), '0'), 38, 10) is null and 
                    src:TRANNO is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRANSACTIONDESIGNATOR), '0'), 38, 10) is null and 
                    src:TRANSACTIONDESIGNATOR is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRANSACTIONTYPE), '0'), 38, 10) is null and 
                    src:TRANSACTIONTYPE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)