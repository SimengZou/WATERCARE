CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_BILLING_REFUND_ERROR AS SELECT src, 'IPS_BILLING_REFUND' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTKEY), '0'), 38, 10) is null and 
                    src:ACCOUNTKEY is not null) THEN
                    'ACCOUNTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ACCOUNTKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPROVALLEVEL), '0'), 38, 10) is null and 
                    src:APPROVALLEVEL is not null) THEN
                    'APPROVALLEVEL contains a non-numeric value : \'' || AS_VARCHAR(src:APPROVALLEVEL) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMMENTKEY), '0'), 38, 10) is null and 
                    src:COMMENTKEY is not null) THEN
                    'COMMENTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:COMMENTKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REFUNDAMOUNT), '0'), 38, 10) is null and 
                    src:REFUNDAMOUNT is not null) THEN
                    'REFUNDAMOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:REFUNDAMOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:REFUNDEDDTTM), '1900-01-01')) is null and 
                    src:REFUNDEDDTTM is not null) THEN
                    'REFUNDEDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:REFUNDEDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REFUNDKEY), '0'), 38, 10) is null and 
                    src:REFUNDKEY is not null) THEN
                    'REFUNDKEY contains a non-numeric value : \'' || AS_VARCHAR(src:REFUNDKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REFUNDLINEITEMSETUPKEY), '0'), 38, 10) is null and 
                    src:REFUNDLINEITEMSETUPKEY is not null) THEN
                    'REFUNDLINEITEMSETUPKEY contains a non-numeric value : \'' || AS_VARCHAR(src:REFUNDLINEITEMSETUPKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:REFUNDSTATUSDTTM), '1900-01-01')) is null and 
                    src:REFUNDSTATUSDTTM is not null) THEN
                    'REFUNDSTATUSDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:REFUNDSTATUSDTTM) || '\'' WHEN 
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
                                    
                src:REFUNDKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_REFUND as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTKEY), '0'), 38, 10) is null and 
                    src:ACCOUNTKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPROVALLEVEL), '0'), 38, 10) is null and 
                    src:APPROVALLEVEL is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMMENTKEY), '0'), 38, 10) is null and 
                    src:COMMENTKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REFUNDAMOUNT), '0'), 38, 10) is null and 
                    src:REFUNDAMOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:REFUNDEDDTTM), '1900-01-01')) is null and 
                    src:REFUNDEDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REFUNDKEY), '0'), 38, 10) is null and 
                    src:REFUNDKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REFUNDLINEITEMSETUPKEY), '0'), 38, 10) is null and 
                    src:REFUNDLINEITEMSETUPKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:REFUNDSTATUSDTTM), '1900-01-01')) is null and 
                    src:REFUNDSTATUSDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)