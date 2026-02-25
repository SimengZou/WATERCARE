CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_BILLING_ADJUSTMENT_ERROR AS SELECT src, 'IPS_BILLING_ADJUSTMENT' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTKEY), '0'), 38, 10) is null and 
                    src:ACCOUNTKEY is not null) THEN
                    'ACCOUNTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ACCOUNTKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADJTYPESUKEY), '0'), 38, 10) is null and 
                    src:ADJTYPESUKEY is not null) THEN
                    'ADJTYPESUKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ADJTYPESUKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADJUSTMENTDTTM), '1900-01-01')) is null and 
                    src:ADJUSTMENTDTTM is not null) THEN
                    'ADJUSTMENTDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADJUSTMENTDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADJUSTMENTKEY), '0'), 38, 10) is null and 
                    src:ADJUSTMENTKEY is not null) THEN
                    'ADJUSTMENTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ADJUSTMENTKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADJUSTMENTSCOUNT), '0'), 38, 10) is null and 
                    src:ADJUSTMENTSCOUNT is not null) THEN
                    'ADJUSTMENTSCOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:ADJUSTMENTSCOUNT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AMOUNT), '0'), 38, 10) is null and 
                    src:AMOUNT is not null) THEN
                    'AMOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:AMOUNT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPROVALAMOUNT), '0'), 38, 10) is null and 
                    src:APPROVALAMOUNT is not null) THEN
                    'APPROVALAMOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:APPROVALAMOUNT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPROVALLEVEL), '0'), 38, 10) is null and 
                    src:APPROVALLEVEL is not null) THEN
                    'APPROVALLEVEL contains a non-numeric value : \'' || AS_VARCHAR(src:APPROVALLEVEL) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLTYPEKEY), '0'), 38, 10) is null and 
                    src:BILLTYPEKEY is not null) THEN
                    'BILLTYPEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:BILLTYPEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMMENTSKEY), '0'), 38, 10) is null and 
                    src:COMMENTSKEY is not null) THEN
                    'COMMENTSKEY contains a non-numeric value : \'' || AS_VARCHAR(src:COMMENTSKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LINEITEMKEY), '0'), 38, 10) is null and 
                    src:LINEITEMKEY is not null) THEN
                    'LINEITEMKEY contains a non-numeric value : \'' || AS_VARCHAR(src:LINEITEMKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PARENTADJUSTMENTKEY), '0'), 38, 10) is null and 
                    src:PARENTADJUSTMENTKEY is not null) THEN
                    'PARENTADJUSTMENTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:PARENTADJUSTMENTKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PENALTYKEY), '0'), 38, 10) is null and 
                    src:PENALTYKEY is not null) THEN
                    'PENALTYKEY contains a non-numeric value : \'' || AS_VARCHAR(src:PENALTYKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STATUSDATE), '1900-01-01')) is null and 
                    src:STATUSDATE is not null) THEN
                    'STATUSDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:STATUSDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRANSACTIONDESIGNATOR), '0'), 38, 10) is null and 
                    src:TRANSACTIONDESIGNATOR is not null) THEN
                    'TRANSACTIONDESIGNATOR contains a non-numeric value : \'' || AS_VARCHAR(src:TRANSACTIONDESIGNATOR) || '\'' WHEN 
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
                                    
                src:ADJUSTMENTKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_ADJUSTMENT as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTKEY), '0'), 38, 10) is null and 
                    src:ACCOUNTKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADJTYPESUKEY), '0'), 38, 10) is null and 
                    src:ADJTYPESUKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADJUSTMENTDTTM), '1900-01-01')) is null and 
                    src:ADJUSTMENTDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADJUSTMENTKEY), '0'), 38, 10) is null and 
                    src:ADJUSTMENTKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADJUSTMENTSCOUNT), '0'), 38, 10) is null and 
                    src:ADJUSTMENTSCOUNT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AMOUNT), '0'), 38, 10) is null and 
                    src:AMOUNT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPROVALAMOUNT), '0'), 38, 10) is null and 
                    src:APPROVALAMOUNT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPROVALLEVEL), '0'), 38, 10) is null and 
                    src:APPROVALLEVEL is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLTYPEKEY), '0'), 38, 10) is null and 
                    src:BILLTYPEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMMENTSKEY), '0'), 38, 10) is null and 
                    src:COMMENTSKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LINEITEMKEY), '0'), 38, 10) is null and 
                    src:LINEITEMKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PARENTADJUSTMENTKEY), '0'), 38, 10) is null and 
                    src:PARENTADJUSTMENTKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PENALTYKEY), '0'), 38, 10) is null and 
                    src:PENALTYKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STATUSDATE), '1900-01-01')) is null and 
                    src:STATUSDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRANSACTIONDESIGNATOR), '0'), 38, 10) is null and 
                    src:TRANSACTIONDESIGNATOR is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)