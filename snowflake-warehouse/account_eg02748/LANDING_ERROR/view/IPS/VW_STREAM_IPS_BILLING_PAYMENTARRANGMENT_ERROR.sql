CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_BILLING_PAYMENTARRANGMENT_ERROR AS SELECT src, 'IPS_BILLING_PAYMENTARRANGMENT' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTKEY), '0'), 38, 10) is null and 
                    src:ACCOUNTKEY is not null) THEN
                    'ACCOUNTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ACCOUNTKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ARRANGEDAMOUNTLTD), '0'), 38, 10) is null and 
                    src:ARRANGEDAMOUNTLTD is not null) THEN
                    'ARRANGEDAMOUNTLTD contains a non-numeric value : \'' || AS_VARCHAR(src:ARRANGEDAMOUNTLTD) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ARRANGEDDTTM), '1900-01-01')) is null and 
                    src:ARRANGEDDTTM is not null) THEN
                    'ARRANGEDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ARRANGEDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ARRANGEDPAYMENTAMT), '0'), 38, 10) is null and 
                    src:ARRANGEDPAYMENTAMT is not null) THEN
                    'ARRANGEDPAYMENTAMT contains a non-numeric value : \'' || AS_VARCHAR(src:ARRANGEDPAYMENTAMT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ARRANGEDTOTALAMT), '0'), 38, 10) is null and 
                    src:ARRANGEDTOTALAMT is not null) THEN
                    'ARRANGEDTOTALAMT contains a non-numeric value : \'' || AS_VARCHAR(src:ARRANGEDTOTALAMT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ARRANGEDVSBILLEDPERCENT), '0'), 38, 10) is null and 
                    src:ARRANGEDVSBILLEDPERCENT is not null) THEN
                    'ARRANGEDVSBILLEDPERCENT contains a non-numeric value : \'' || AS_VARCHAR(src:ARRANGEDVSBILLEDPERCENT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ARRANGEMENTBALANCE), '0'), 38, 10) is null and 
                    src:ARRANGEMENTBALANCE is not null) THEN
                    'ARRANGEMENTBALANCE contains a non-numeric value : \'' || AS_VARCHAR(src:ARRANGEMENTBALANCE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ARRANGEMENTTEMPLATEKEY), '0'), 38, 10) is null and 
                    src:ARRANGEMENTTEMPLATEKEY is not null) THEN
                    'ARRANGEMENTTEMPLATEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ARRANGEMENTTEMPLATEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLEDAMOUNTLTD), '0'), 38, 10) is null and 
                    src:BILLEDAMOUNTLTD is not null) THEN
                    'BILLEDAMOUNTLTD contains a non-numeric value : \'' || AS_VARCHAR(src:BILLEDAMOUNTLTD) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DELINQUENTBALANCE), '0'), 38, 10) is null and 
                    src:DELINQUENTBALANCE is not null) THEN
                    'DELINQUENTBALANCE contains a non-numeric value : \'' || AS_VARCHAR(src:DELINQUENTBALANCE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DOWNPAYMENTAMT), '0'), 38, 10) is null and 
                    src:DOWNPAYMENTAMT is not null) THEN
                    'DOWNPAYMENTAMT contains a non-numeric value : \'' || AS_VARCHAR(src:DOWNPAYMENTAMT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DOWNPAYMENTDUEDTTM), '1900-01-01')) is null and 
                    src:DOWNPAYMENTDUEDTTM is not null) THEN
                    'DOWNPAYMENTDUEDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:DOWNPAYMENTDUEDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ENDDTTM), '1900-01-01')) is null and 
                    src:ENDDTTM is not null) THEN
                    'ENDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ENDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:GUARANTORKEY), '0'), 38, 10) is null and 
                    src:GUARANTORKEY is not null) THEN
                    'GUARANTORKEY contains a non-numeric value : \'' || AS_VARCHAR(src:GUARANTORKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMBEROFPAYMENTS), '0'), 38, 10) is null and 
                    src:NUMBEROFPAYMENTS is not null) THEN
                    'NUMBEROFPAYMENTS contains a non-numeric value : \'' || AS_VARCHAR(src:NUMBEROFPAYMENTS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PARENTARRANGEMENTKEY), '0'), 38, 10) is null and 
                    src:PARENTARRANGEMENTKEY is not null) THEN
                    'PARENTARRANGEMENTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:PARENTARRANGEMENTKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYMENTARRANGMENTKEY), '0'), 38, 10) is null and 
                    src:PAYMENTARRANGMENTKEY is not null) THEN
                    'PAYMENTARRANGMENTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:PAYMENTARRANGMENTKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STARTDTTM), '1900-01-01')) is null and 
                    src:STARTDTTM is not null) THEN
                    'STARTDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:STARTDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STATUSDTTM), '1900-01-01')) is null and 
                    src:STATUSDTTM is not null) THEN
                    'STATUSDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:STATUSDTTM) || '\'' WHEN 
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
                                    
                src:PAYMENTARRANGMENTKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_PAYMENTARRANGMENT as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTKEY), '0'), 38, 10) is null and 
                    src:ACCOUNTKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ARRANGEDAMOUNTLTD), '0'), 38, 10) is null and 
                    src:ARRANGEDAMOUNTLTD is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ARRANGEDDTTM), '1900-01-01')) is null and 
                    src:ARRANGEDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ARRANGEDPAYMENTAMT), '0'), 38, 10) is null and 
                    src:ARRANGEDPAYMENTAMT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ARRANGEDTOTALAMT), '0'), 38, 10) is null and 
                    src:ARRANGEDTOTALAMT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ARRANGEDVSBILLEDPERCENT), '0'), 38, 10) is null and 
                    src:ARRANGEDVSBILLEDPERCENT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ARRANGEMENTBALANCE), '0'), 38, 10) is null and 
                    src:ARRANGEMENTBALANCE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ARRANGEMENTTEMPLATEKEY), '0'), 38, 10) is null and 
                    src:ARRANGEMENTTEMPLATEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLEDAMOUNTLTD), '0'), 38, 10) is null and 
                    src:BILLEDAMOUNTLTD is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DELINQUENTBALANCE), '0'), 38, 10) is null and 
                    src:DELINQUENTBALANCE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DOWNPAYMENTAMT), '0'), 38, 10) is null and 
                    src:DOWNPAYMENTAMT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DOWNPAYMENTDUEDTTM), '1900-01-01')) is null and 
                    src:DOWNPAYMENTDUEDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ENDDTTM), '1900-01-01')) is null and 
                    src:ENDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:GUARANTORKEY), '0'), 38, 10) is null and 
                    src:GUARANTORKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMBEROFPAYMENTS), '0'), 38, 10) is null and 
                    src:NUMBEROFPAYMENTS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PARENTARRANGEMENTKEY), '0'), 38, 10) is null and 
                    src:PARENTARRANGEMENTKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYMENTARRANGMENTKEY), '0'), 38, 10) is null and 
                    src:PAYMENTARRANGMENTKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STARTDTTM), '1900-01-01')) is null and 
                    src:STARTDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STATUSDTTM), '1900-01-01')) is null and 
                    src:STATUSDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)