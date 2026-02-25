CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_BILLING_PAYMENTIMPORTRUNDETAILS_ERROR AS SELECT src, 'IPS_BILLING_PAYMENTIMPORTRUNDETAILS' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BATCHKEY), '0'), 38, 10) is null and 
                    src:BATCHKEY is not null) THEN
                    'BATCHKEY contains a non-numeric value : \'' || AS_VARCHAR(src:BATCHKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYMENTEXCEPTIONCOUNT), '0'), 38, 10) is null and 
                    src:PAYMENTEXCEPTIONCOUNT is not null) THEN
                    'PAYMENTEXCEPTIONCOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:PAYMENTEXCEPTIONCOUNT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYMENTIMPORTRUNDETAILSKEY), '0'), 38, 10) is null and 
                    src:PAYMENTIMPORTRUNDETAILSKEY is not null) THEN
                    'PAYMENTIMPORTRUNDETAILSKEY contains a non-numeric value : \'' || AS_VARCHAR(src:PAYMENTIMPORTRUNDETAILSKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYMENTIMPORTRUNKEY), '0'), 38, 10) is null and 
                    src:PAYMENTIMPORTRUNKEY is not null) THEN
                    'PAYMENTIMPORTRUNKEY contains a non-numeric value : \'' || AS_VARCHAR(src:PAYMENTIMPORTRUNKEY) || '\'' WHEN 
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
                                    
                src:PAYMENTIMPORTRUNDETAILSKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_PAYMENTIMPORTRUNDETAILS as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BATCHKEY), '0'), 38, 10) is null and 
                    src:BATCHKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYMENTEXCEPTIONCOUNT), '0'), 38, 10) is null and 
                    src:PAYMENTEXCEPTIONCOUNT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYMENTIMPORTRUNDETAILSKEY), '0'), 38, 10) is null and 
                    src:PAYMENTIMPORTRUNDETAILSKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYMENTIMPORTRUNKEY), '0'), 38, 10) is null and 
                    src:PAYMENTIMPORTRUNKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)